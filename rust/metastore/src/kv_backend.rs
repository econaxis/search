use std::collections::HashMap;

use crate::mvcc_metadata::MVCCMetadata;
use crate::{LockDataRef, TransactionLockData};

#[derive(Debug)]
pub struct ValueWithMVCC(pub MVCCMetadata, pub String);

pub type IntentMapType = HashMap<LockDataRef, TransactionLockData>;
