use std::time::SystemTime;
use std::fmt::{Display, Formatter, Debug};
use tracing::field::{Field, Visit};

pub struct TimeSpan(SystemTime);

impl TimeSpan {
    pub fn new() -> Self {
        Self(SystemTime::now())
    }
    pub fn elapsed(&self) -> u32 {
        self.0.elapsed().unwrap().as_millis() as u32
    }
}

pub fn new_span() -> TimeSpan {
    TimeSpan::new()
}

impl Display for TimeSpan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
