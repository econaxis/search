use crate::rwtransaction_wrapper::MVCCMetadata;
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub struct ValueWithMVCC(pub MVCCMetadata, pub String);

