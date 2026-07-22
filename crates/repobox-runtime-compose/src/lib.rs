//! Docker Compose detection and execution.

mod detect;
mod driver;
mod model;

pub use detect::{detect_compose_files, detect_configuration, detect_repository};
pub use driver::ComposeRuntime;
