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
                let (meta, string) = txn.write(&k, v.1.clone().into())?.as_inner();

                assert_eq!(string, v.1);
                assert!(meta.sorta_equal(&v.0));
            }
            Operation::Read(k) => {
                txn.read(k.as_cow_str())?;
            }
        }
    };

    txn.commit();

    Ok(())
}
