use std::time::SystemTime;
use std::fmt::{Display, Formatter, Debug};

pub struct TimeSpan(SystemTime);

impl TimeSpan {
    pub fn new() -> Self {
        Self(SystemTime::now())
    }
    pub fn elapsed(&self) -> u32 {
        self.0.elapsed().unwrap().as_millis() as u32
    }
}

impl Display for TimeSpan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}