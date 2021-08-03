use crate::DbContext;

use crate::rwtransaction_wrapper::RWTransactionWrapper;
use crate::wal_watcher::Operation;

use super::WalTxn;

pub fn apply_wal_txn_checked(waltxn: WalTxn, ctx: &DbContext) -> Result<(), String> {
    let mut txn = RWTransactionWrapper::new_with_time(ctx, waltxn.timestamp);

    for op in waltxn.ops {
        match op {
            Operation::Write(k, v) => {
                let v = v.as_inner();
                let (_meta, string) = txn.write(&k, v.1.clone().into()).unwrap().as_inner();

                assert_eq!(string, v.1);
            }
            Operation::Read(k, mut v) => {
                let mut v1 = txn.read_mvcc(k.as_cow_str()).map_err(|err| format!("{} {}", err, k.as_str()))?;
                assert_eq!(v1.get_readable().unwrap().val, v.get_readable().unwrap().val);
            }
        }
    }

    txn.commit();

    Ok(())
}
