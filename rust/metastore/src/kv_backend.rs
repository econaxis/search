


use crate::mvcc_manager::MVCCMetadata;


#[derive(Debug)]
pub struct ValueWithMVCC(pub MVCCMetadata, pub String);
