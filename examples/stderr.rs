use std::io;

use sawmill::{Builder, LevelFilter, StderrAppender};

fn main() -> io::Result<()> {
    let guard = Builder::new(LevelFilter::Info).init(StderrAppender)?;

    log::info!("Application started");
    log::debug!("Hidden at the Info level");
    log::warn!("Application is shutting down");

    guard.shutdown()
}
