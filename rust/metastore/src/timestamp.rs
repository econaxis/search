use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
#[derive(Eq, PartialEq, Copy, Clone, Debug, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(match (self.0, other.0) {
            (0, 0) => Ordering::Equal,
            (0, _left) => Ordering::Greater,
            (_right, 0) => Ordering::Less,
            (left, right) => left.cmp(&right),
        })
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

static MONOTIC_COUNTER: AtomicU64 = AtomicU64::new(100);

impl Timestamp {
    pub fn mintime() -> Self {
        Self(1)
    }
    pub fn maxtime() -> Self {
        Self(0)
    }

    pub fn now() -> Self {
        Self(MONOTIC_COUNTER.fetch_add(1, AtomicOrdering::SeqCst))
    }
}

impl ToString for Timestamp {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl From<u64> for Timestamp {
    fn from(a: u64) -> Self {
        Self(a)
    }
}
