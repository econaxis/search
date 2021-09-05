use crate::DbContext;

use crate::rwtransaction_wrapper::{ReplicatedTxn, Transaction};
use crate::wal_watcher::Operation;

use super::WalTxn;

pub fn apply_wal_txn_checked(waltxn: WalTxn, ctx: &DbContext) {
    assert!(ctx.replicators.is_none());
    let mut txn = Transaction::new_with_time(ctx, waltxn.timestamp);

    for op in waltxn.ops {
        match op {
            Operation::Write(k, v) => {
                txn.write(&ctx, &k, v).unwrap();
            }
            Operation::Read(k, v) => {
                let v1 = txn
                    .read_mvcc(&ctx, &k)
                    .map_err(|err| format!("{} {}", err, k.as_str()))
                    .unwrap();
                let value1 = v1.get_val();
                if value1 != &v {
                    println!("read error! non matching {:?} {:?}", value1, v);
                    panic!("Read error");
                }
            }
        }
    }

    txn.commit(&ctx).unwrap();
}
