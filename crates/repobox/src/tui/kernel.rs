use std::io::{self, BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded};
use crossterm::cursor::{Hide, Show};
use crossterm::event::{
    DisableBracketedPaste, DisableFocusChange, DisableMouseCapture, EnableBracketedPaste,
    EnableFocusChange, EnableMouseCapture, Event,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use repobox_core::{ErrorKind, RepoboxError, Result};
use tokio::sync::mpsc;

const FRAME_INTERVAL: Duration = Duration::from_millis(16);
const SYNC_BEGIN: &[u8] = b"\x1b[?2026h";
const SYNC_END: &[u8] = b"\x1b[?2026l";

enum WriterMessage {
    Frame { sequence: u64, bytes: Vec<u8> },
    Shutdown,
}

pub struct FrameWriter {
    buffer: Vec<u8>,
    sender: Sender<WriterMessage>,
    sequence: u64,
}

impl Write for FrameWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sequence += 1;
        let bytes = std::mem::replace(&mut self.buffer, Vec::with_capacity(64 * 1024));
        self.sender
            .send(WriterMessage::Frame {
                sequence: self.sequence,
                bytes,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "terminal writer stopped"))
    }
}

impl Drop for FrameWriter {
    fn drop(&mut self) {
        let _ = self.sender.send(WriterMessage::Shutdown);
    }
}

pub struct TerminalKernel {
    pub terminal: Terminal<CrosstermBackend<FrameWriter>>,
    input: mpsc::UnboundedReceiver<Event>,
    acknowledgements: mpsc::UnboundedReceiver<u64>,
    stop_input: Arc<AtomicBool>,
    writer_thread: Option<JoinHandle<()>>,
    presenter: Presenter,
    _guard: TerminalGuard,
}

pub enum KernelEvent {
    Input(Event),
    FrameWritten(u64),
    Paint,
    Closed,
}

impl TerminalKernel {
    pub fn enter() -> Result<Self> {
        let guard = TerminalGuard::enter()?;
        let (writer, acknowledgements, writer_thread) = spawn_writer();
        let backend = CrosstermBackend::new(writer);
        let mut terminal = Terminal::new(backend).map_err(terminal_error)?;
        terminal.clear().map_err(terminal_error)?;
        let (input, stop_input) = spawn_input();
        let presenter = Presenter {
            frame_in_flight: true,
            ..Presenter::default()
        };
        Ok(Self {
            terminal,
            input,
            acknowledgements,
            stop_input,
            writer_thread: Some(writer_thread),
            presenter,
            _guard: guard,
        })
    }

    pub async fn next(&mut self) -> KernelEvent {
        let delay = self.presenter.next_paint_delay();
        tokio::select! {
            input = self.input.recv() => input.map_or(KernelEvent::Closed, KernelEvent::Input),
            acknowledgement = self.acknowledgements.recv() => {
                acknowledgement.map_or(KernelEvent::Closed, KernelEvent::FrameWritten)
            }
            () = tokio::time::sleep(delay) => KernelEvent::Paint,
        }
    }

    pub fn mark_dirty(&mut self, immediate: bool) {
        self.presenter.mark_dirty(immediate);
    }

    pub fn acknowledge(&mut self, sequence: u64) {
        self.presenter.acknowledge(sequence);
    }

    pub fn can_paint(&self) -> bool {
        self.presenter.can_paint()
    }

    pub fn painted(&mut self) {
        self.presenter.painted();
    }
}

impl Drop for TerminalKernel {
    fn drop(&mut self) {
        self.stop_input.store(true, Ordering::Release);
        // Dropping the terminal drops FrameWriter, which sends Shutdown. The
        // join happens after Rust drops fields, so the normal terminal guard
        // still restores modes even if the writer is already unavailable.
        if let Some(thread) = self.writer_thread.take() {
            // The sender is owned by the terminal backend and is dropped after
            // this Drop body. Do not block indefinitely waiting for it here.
            if thread.is_finished() {
                let _ = thread.join();
            }
        }
    }
}

#[derive(Debug)]
struct Presenter {
    dirty: bool,
    frame_in_flight: bool,
    last_frame: Instant,
    last_sequence: u64,
    immediate: bool,
}

impl Default for Presenter {
    fn default() -> Self {
        Self {
            dirty: true,
            frame_in_flight: false,
            last_frame: Instant::now()
                .checked_sub(FRAME_INTERVAL)
                .unwrap_or_else(Instant::now),
            last_sequence: 0,
            immediate: true,
        }
    }
}

impl Presenter {
    fn mark_dirty(&mut self, immediate: bool) {
        self.dirty = true;
        self.immediate |= immediate;
    }

    fn can_paint(&self) -> bool {
        self.dirty
            && !self.frame_in_flight
            && (self.immediate || self.last_frame.elapsed() >= FRAME_INTERVAL)
    }

    fn next_paint_delay(&self) -> Duration {
        if !self.dirty || self.frame_in_flight {
            return Duration::from_hours(24);
        }
        if self.immediate {
            return Duration::ZERO;
        }
        FRAME_INTERVAL.saturating_sub(self.last_frame.elapsed())
    }

    fn painted(&mut self) {
        self.dirty = false;
        self.frame_in_flight = true;
        self.immediate = false;
        self.last_frame = Instant::now();
    }

    fn acknowledge(&mut self, sequence: u64) {
        if sequence >= self.last_sequence {
            self.last_sequence = sequence;
            self.frame_in_flight = false;
        }
    }
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().map_err(terminal_error)?;
        if let Err(error) = execute!(
            io::stderr(),
            EnterAlternateScreen,
            EnableBracketedPaste,
            EnableFocusChange,
            EnableMouseCapture,
            Hide
        ) {
            let _ = disable_raw_mode();
            return Err(terminal_error(error));
        }
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(
            io::stderr(),
            Show,
            DisableMouseCapture,
            DisableFocusChange,
            DisableBracketedPaste,
            LeaveAlternateScreen
        );
        let _ = disable_raw_mode();
    }
}

fn spawn_writer() -> (FrameWriter, mpsc::UnboundedReceiver<u64>, JoinHandle<()>) {
    let (sender, receiver): (Sender<WriterMessage>, Receiver<WriterMessage>) = bounded(1);
    let (ack_sender, ack_receiver) = mpsc::unbounded_channel();
    let thread = thread::Builder::new()
        .name("repobox-terminal-writer".to_owned())
        .spawn(move || {
            let mut output = BufWriter::with_capacity(64 * 1024, io::stderr());
            while let Ok(message) = receiver.recv() {
                match message {
                    WriterMessage::Frame { sequence, bytes } => {
                        let result = output
                            .write_all(SYNC_BEGIN)
                            .and_then(|()| output.write_all(&bytes))
                            .and_then(|()| output.write_all(SYNC_END))
                            .and_then(|()| output.flush());
                        if result.is_err() || ack_sender.send(sequence).is_err() {
                            break;
                        }
                    }
                    WriterMessage::Shutdown => break,
                }
            }
        })
        .expect("terminal writer thread can start");
    (
        FrameWriter {
            buffer: Vec::with_capacity(64 * 1024),
            sender,
            sequence: 0,
        },
        ack_receiver,
        thread,
    )
}

fn spawn_input() -> (mpsc::UnboundedReceiver<Event>, Arc<AtomicBool>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    thread::Builder::new()
        .name("repobox-terminal-input".to_owned())
        .spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match crossterm::event::poll(Duration::from_millis(100)) {
                    Ok(true) => match crossterm::event::read() {
                        Ok(event) => {
                            if sender.send(event).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    },
                    Ok(false) => {}
                    Err(_) => break,
                }
            }
        })
        .expect("terminal input thread can start");
    (receiver, stop)
}

fn terminal_error(error: io::Error) -> RepoboxError {
    RepoboxError::new(
        ErrorKind::Runtime,
        "terminal_error",
        format!("terminal operation failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presenter_coalesces_while_frame_is_in_flight() {
        let mut presenter = Presenter::default();
        assert!(presenter.can_paint());
        presenter.painted();
        presenter.mark_dirty(false);
        assert!(!presenter.can_paint());
        presenter.acknowledge(1);
        assert!(presenter.dirty);
    }
}
