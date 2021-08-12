use crate::DbContext;

use crate::rwtransaction_wrapper::ReplicatedTxn;
use crate::wal_watcher::Operation;

use super::WalTxn;

pub fn apply_wal_txn_checked(waltxn: WalTxn, ctx: &DbContext) -> Result<(), String> {
    let mut txn = ReplicatedTxn::new_with_time(ctx, waltxn.timestamp);

    for op in waltxn.ops {
        match op {
            Operation::Write(k, v) => {
                let v = v.as_inner();
                txn.write(&k, v.1.clone().into()).unwrap();
            }
            Operation::Read(k, v) => {
                let v1 = txn.read_mvcc(&k).map_err(|err| format!("{} {}", err, k.as_str()))?;
                let value1 = v1.into_inner();
                let value =  v.into_inner();
                if !(value1.1 == value.1) {
                    println!("read error! non matching {:?} {:?}", value1, value);
                    return Err("Read error".to_string())
                }
            }
        }
    }

    txn.commit();

    Ok(())
}
