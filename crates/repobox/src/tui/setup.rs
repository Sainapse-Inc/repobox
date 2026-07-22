use std::io::{self, IsTerminal};

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Margin};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use repobox_core::runtime::{DetectedServiceKind, RuntimeDetection};
use repobox_core::{ErrorKind, RepoboxError, Result};

use super::kernel::{KernelEvent, TerminalKernel};

const BG: Color = Color::Rgb(9, 12, 18);
const PANEL: Color = Color::Rgb(17, 22, 32);
const TEXT: Color = Color::Rgb(226, 232, 240);
const MUTED: Color = Color::Rgb(122, 133, 153);
const CYAN: Color = Color::Rgb(91, 206, 250);
const GREEN: Color = Color::Rgb(80, 227, 164);

pub async fn select_organization(
    organizations: &[String],
    detection: &RuntimeDetection,
) -> Result<String> {
    if organizations.is_empty() {
        return Err(RepoboxError::new(
            ErrorKind::NotFound,
            "planetscale_organization_not_found",
            "the service token cannot access any PlanetScale organization",
        ));
    }
    if !io::stdin().is_terminal() || !io::stderr().is_terminal() {
        if organizations.len() == 1 {
            return Ok(organizations[0].clone());
        }
        return Err(RepoboxError::new(
            ErrorKind::Usage,
            "organization_required",
            format!(
                "--organization is required; available organizations: {}",
                organizations.join(", ")
            ),
        ));
    }

    let mut kernel = TerminalKernel::enter()?;
    let mut selected = 0_usize;
    loop {
        match kernel.next().await {
            KernelEvent::Input(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => selected = selected.saturating_sub(1),
                    KeyCode::Down | KeyCode::Char('j') => {
                        selected = (selected + 1).min(organizations.len() - 1);
                    }
                    KeyCode::Enter => return Ok(organizations[selected].clone()),
                    KeyCode::Esc | KeyCode::Char('q') => {
                        return Err(RepoboxError::new(
                            ErrorKind::Usage,
                            "setup_canceled",
                            "Repobox setup was canceled",
                        ));
                    }
                    _ => {}
                }
                kernel.mark_dirty(true);
            }
            KernelEvent::Input(Event::Resize(_, _)) => kernel.mark_dirty(false),
            KernelEvent::FrameWritten(sequence) => kernel.acknowledge(sequence),
            KernelEvent::Paint if kernel.can_paint() => {
                kernel
                    .terminal
                    .draw(|frame| draw(frame, organizations, selected, detection))
                    .map_err(|error| {
                        RepoboxError::new(
                            ErrorKind::Runtime,
                            "terminal_render_failed",
                            error.to_string(),
                        )
                    })?;
                kernel.painted();
            }
            KernelEvent::Closed => {
                return Err(RepoboxError::new(
                    ErrorKind::Runtime,
                    "terminal_input_closed",
                    "terminal input closed during setup",
                ));
            }
            KernelEvent::Input(_) | KernelEvent::Paint => {}
        }
    }
}

fn draw(
    frame: &mut Frame<'_>,
    organizations: &[String],
    selected: usize,
    detection: &RuntimeDetection,
) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::default().bg(BG)), area);
    let shell = area.inner(Margin::new(3, 2));
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(5),
            Constraint::Min(7),
            Constraint::Length(3),
        ])
        .split(shell);

    let title = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                "repo",
                Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "box",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(Span::styled(
            "Persistent data for every branch",
            Style::default().fg(MUTED),
        )),
    ]);
    frame.render_widget(title, chunks[0]);

    let postgres = detection
        .services
        .iter()
        .filter(|service| service.kind == DetectedServiceKind::Postgres)
        .map(|service| service.name.as_str())
        .collect::<Vec<_>>();
    let summary = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("Detected  ", Style::default().fg(MUTED)),
            Span::styled(detection.driver.as_str(), Style::default().fg(GREEN)),
        ]),
        Line::from(vec![
            Span::styled("Postgres  ", Style::default().fg(MUTED)),
            Span::styled(
                if postgres.is_empty() {
                    "none".to_owned()
                } else {
                    postgres.join(", ")
                },
                Style::default().fg(TEXT),
            ),
        ]),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(PANEL)),
    )
    .wrap(Wrap { trim: true });
    frame.render_widget(summary, chunks[1]);

    let items = organizations
        .iter()
        .map(|organization| ListItem::new(format!("  {organization}")))
        .collect::<Vec<_>>();
    let list = List::new(items)
        .block(
            Block::default()
                .title(" PlanetScale organization ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(CYAN))
                .style(Style::default().bg(PANEL).fg(TEXT)),
        )
        .highlight_symbol("› ")
        .highlight_style(
            Style::default()
                .fg(CYAN)
                .add_modifier(Modifier::BOLD | Modifier::REVERSED),
        );
    let mut state = ListState::default().with_selected(Some(selected));
    frame.render_stateful_widget(list, chunks[2], &mut state);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("↑↓", Style::default().fg(CYAN)),
            Span::styled(" move   ", Style::default().fg(MUTED)),
            Span::styled("enter", Style::default().fg(CYAN)),
            Span::styled(" select   ", Style::default().fg(MUTED)),
            Span::styled("q", Style::default().fg(CYAN)),
            Span::styled(" cancel", Style::default().fg(MUTED)),
        ]))
        .alignment(Alignment::Center),
        chunks[3],
    );
}
