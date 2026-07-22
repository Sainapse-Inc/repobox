use std::collections::VecDeque;

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Margin};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use repobox_core::{ErrorKind, RepoboxError, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::kernel::{KernelEvent, TerminalKernel};

const BG: Color = Color::Rgb(8, 11, 17);
const PANEL: Color = Color::Rgb(15, 20, 29);
const TEXT: Color = Color::Rgb(226, 232, 240);
const MUTED: Color = Color::Rgb(119, 130, 150);
const CYAN: Color = Color::Rgb(91, 206, 250);
const GREEN: Color = Color::Rgb(80, 227, 164);
const AMBER: Color = Color::Rgb(250, 194, 91);

#[derive(Clone, Copy, Debug, Default)]
enum ProcessState {
    #[default]
    Running,
    Exited(Option<i32>),
}

#[derive(Clone, Debug)]
pub struct DashboardOptions {
    pub project: String,
    pub environment: String,
    pub services: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum DashboardEvent {
    Log { service: String, line: String },
    ServiceState { service: String, state: String },
    Notice { message: String },
    ProcessExited { code: Option<i32> },
}

pub async fn run_dashboard(
    options: DashboardOptions,
    mut events: mpsc::Receiver<DashboardEvent>,
) -> Result<()> {
    let mut kernel = TerminalKernel::enter()?;
    let mut logs = VecDeque::with_capacity(2_000);
    let mut scroll = 0_usize;
    let mut paused = false;
    let mut process_state = ProcessState::Running;
    let mut events_open = true;
    loop {
        tokio::select! {
            biased;
            event = kernel.next() => {
                match event {
                    KernelEvent::Input(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                            KeyCode::Char('c') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => return Ok(()),
                            KeyCode::Char(' ') => paused = !paused,
                            KeyCode::Up | KeyCode::Char('k') => scroll = scroll.saturating_add(1),
                            KeyCode::Down | KeyCode::Char('j') => scroll = scroll.saturating_sub(1),
                            KeyCode::Home => scroll = logs.len().saturating_sub(1),
                            KeyCode::End => scroll = 0,
                            _ => {}
                        }
                        kernel.mark_dirty(true);
                    }
                    KernelEvent::Input(Event::Resize(_, _)) => kernel.mark_dirty(false),
                    KernelEvent::FrameWritten(sequence) => kernel.acknowledge(sequence),
                    KernelEvent::Paint if kernel.can_paint() => {
                        kernel.terminal.draw(|frame| {
                            draw(frame, &options, &logs, scroll, paused, process_state);
                        }).map_err(|error| RepoboxError::new(
                            ErrorKind::Runtime,
                            "terminal_render_failed",
                            error.to_string(),
                        ))?;
                        kernel.painted();
                    }
                    KernelEvent::Closed => return Ok(()),
                    KernelEvent::Input(_) | KernelEvent::Paint => {}
                }
            }
            event = events.recv(), if events_open => {
                let Some(event) = event else {
                    events_open = false;
                    kernel.mark_dirty(true);
                    continue;
                };
                apply_event(event, &mut logs, &mut process_state);
                for _ in 0..31 {
                    let Ok(event) = events.try_recv() else { break };
                    apply_event(event, &mut logs, &mut process_state);
                }
                if !paused {
                    scroll = 0;
                }
                kernel.mark_dirty(false);
            }
        }
    }
}

fn apply_event(
    event: DashboardEvent,
    logs: &mut VecDeque<(String, String)>,
    process_state: &mut ProcessState,
) {
    match event {
        DashboardEvent::Log { service, line } => push_log(logs, service, line),
        DashboardEvent::ServiceState { service, state } => {
            push_log(logs, "repobox".to_owned(), format!("{service}: {state}"));
        }
        DashboardEvent::Notice { message } => {
            push_log(logs, "repobox".to_owned(), message);
        }
        DashboardEvent::ProcessExited { code } => {
            *process_state = ProcessState::Exited(code);
            push_log(
                logs,
                "repobox".to_owned(),
                format!(
                    "log process exited{}",
                    code.map_or(String::new(), |code| format!(" ({code})"))
                ),
            );
        }
    }
}

fn push_log(logs: &mut VecDeque<(String, String)>, service: String, line: String) {
    if logs.len() == 2_000 {
        logs.pop_front();
    }
    logs.push_back((service, line));
}

fn draw(
    frame: &mut Frame<'_>,
    options: &DashboardOptions,
    logs: &VecDeque<(String, String)>,
    scroll: usize,
    paused: bool,
    process_state: ProcessState,
) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::default().bg(BG)), area);
    let shell = area.inner(Margin::new(2, 1));
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(4),
            Constraint::Min(5),
            Constraint::Length(2),
        ])
        .split(shell);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "repo",
                Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "box",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            ),
            Span::styled("  /  ", Style::default().fg(MUTED)),
            Span::styled(&options.project, Style::default().fg(TEXT)),
            Span::styled("  /  ", Style::default().fg(MUTED)),
            Span::styled(&options.environment, Style::default().fg(GREEN)),
        ])),
        rows[0],
    );

    let status = if matches!(process_state, ProcessState::Exited(_)) {
        ("logs ended", AMBER)
    } else if paused {
        ("paused", AMBER)
    } else {
        ("connected", GREEN)
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled("● ", Style::default().fg(status.1)),
                Span::styled(status.0, Style::default().fg(status.1)),
                Span::styled("    services  ", Style::default().fg(MUTED)),
                Span::styled(options.services.join("  "), Style::default().fg(TEXT)),
            ]),
            Line::from(Span::styled(
                "Remote Postgres is persistent; application services are local.",
                Style::default().fg(MUTED),
            )),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(PANEL)),
        ),
        rows[1],
    );

    let height = rows[2].height.saturating_sub(2) as usize;
    let end = logs.len().saturating_sub(scroll.min(logs.len()));
    let start = end.saturating_sub(height);
    let lines = logs
        .iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .map(|(service, line)| {
            Line::from(vec![
                Span::styled(format!("{service:>12}  "), Style::default().fg(CYAN)),
                Span::styled(line, Style::default().fg(TEXT)),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(" logs ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(PANEL))
                    .style(Style::default().bg(PANEL)),
            )
            .wrap(Wrap { trim: false }),
        rows[2],
    );

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("↑↓", Style::default().fg(CYAN)),
            Span::styled(" scroll   ", Style::default().fg(MUTED)),
            Span::styled("space", Style::default().fg(CYAN)),
            Span::styled(" pause   ", Style::default().fg(MUTED)),
            Span::styled("q", Style::default().fg(CYAN)),
            Span::styled(" leave", Style::default().fg(MUTED)),
        ]))
        .alignment(Alignment::Center),
        rows[3],
    );

    if let ProcessState::Exited(code) = process_state {
        let message = code.map_or_else(
            || "log stream ended".to_owned(),
            |code| format!("log stream exited with {code}"),
        );
        frame.render_widget(
            Paragraph::new(message)
                .alignment(Alignment::Right)
                .style(Style::default().fg(AMBER)),
            rows[3],
        );
    }
}
