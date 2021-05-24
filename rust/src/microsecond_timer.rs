use tracing_subscriber::fmt::{time, self};
use std::fmt::{write, Write, Result};
use chrono;

pub struct MicrosecondTimer {}

impl time::FormatTime for MicrosecondTimer {
    fn format_time(&self, w: &mut dyn Write) -> Result {
        write!(w, "{}", chrono::Local::now().format("%I:%M:%S%.3f"));
        Ok(())
    }
}