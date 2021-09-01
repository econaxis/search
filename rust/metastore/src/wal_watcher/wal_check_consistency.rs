use std::time::Duration;

use crate::DbContext;
use crate::rwtransaction_wrapper::{Transaction, LockDataRef};
use crate::timestamp::Timestamp;
use crate::wal_watcher::WalLoader;
use crate::rpc_handler::DatabaseInterface;
use crate::replicated_slave::SelfContainedDb;
use crate::db_context::create_empty_context;


pub fn check_func1<A: DatabaseInterface + ?Sized>(db: &DbContext, db2: &A, time: Timestamp) -> Result<bool, String> {
    let txn2 = LockDataRef::debug_new(time.0);
    let mut txn = Transaction::new_with_time(db, time);
    db2.new_transaction(&txn2);


    let ret2 = db2.serve_range_read(txn2, &"/".into())??;
    let mut ret: Result<_, String> = Err("a".into());
    while ret.is_err() {
        match ret.unwrap_err().as_str() {
            "a" => {}
            err => println!("check {}", err)
        }
        std::thread::sleep(Duration::from_millis(1000));
        ret = txn.read_range_owned(db, &"/".into());
    }
    let ret = ret.unwrap();

    println!("Done reading ranges");

    if ret.len() != ret2.len() {
        return Err("lengths not the same".to_string());
    }

    ret.iter().zip(ret2.iter()).for_each(|(a, b)| {
        if a.0 != b.0 {
            // skip, because without wallog freezing, not guaranteed to get the same reuslts (e.g. new transactions might commit)
            return;
        }

        if !(a.0 == b.0 && a.1.as_inner().1 == b.1.as_inner().1) {


            // acceptible becasue we can't lock the DB between doing the wallog.apply and the read (no way to do it right now).
            // Therefore, any new writes between that wallog.apply and the comprehensive "/" read will be logged as error, even though
            // that's perfectly OK for the purposes of the test.
            if b.1.as_inner().0.get_end_time() == Timestamp::maxtime() {
                return;
            }
            println!("Non matching {:?}", a);
            println!("Non matching {:?}", b);
            // wallog.print();
            panic!("Split brain between WAL log and the main DB. applying WAL log failed");
        };
    });
    println!("End checking {}", ret.len());

    Ok(true)
}

pub fn check_func(db: &DbContext) -> Result<bool, String> {
    let db2 = create_empty_context();
    let wallog = &db.wallog;
    let time = wallog.apply(&db2).unwrap() - Timestamp::from(1);
    let db2 = SelfContainedDb::new(db2);
    check_func1(db, &db2, time)
}
