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
                let t0 = txn.write(&k, v.1.clone().into()).unwrap();
                let (_meta, string) = t0.as_inner();

                assert_eq!(string, v.1);
            }
            Operation::Read(k, mut v) => {
                let mut v1 = txn.read_mvcc(&k).map_err(|err| format!("{} {}", err, k.as_str()))?;
                if !(v1.get_readable().unwrap().val == v.get_readable().unwrap().val) {
                    println!("read error! non matching {:?} {:?}", v1, v);
                    return Err("Read error".to_string())
                }
            }
        }
    }

    txn.commit();

    Ok(())
}
