use std::time::Duration;

use crate::DbContext;
use crate::rwtransaction_wrapper::{Transaction};
use crate::timestamp::Timestamp;
use crate::wal_watcher::WalLoader;

pub fn check_func1(db: &DbContext, db2: &DbContext, time: Timestamp) -> Result<bool, String> {
    let mut txn2 = Transaction::new_with_time(db2, time);
    let ret2 = txn2.read_range_owned(db2, &"/".into())?;
    let mut txn = Transaction::new_with_time(db, time);
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
    // db.wallog.unfreeze();

    println!("Done reading ranges");

    ret.iter().zip(ret2.iter()).for_each(|(a, b)| {
        if a.0 != b.0 {
            // skip, because without wallog freezing, not guaranteed to get the same reuslts (e.g. new transactions might commit)
            return;
        }

        if !(a.0 == b.0 && a.1.as_inner().1 == b.1.as_inner().1) {

            if b.1.as_inner().0.get_end_time() == Timestamp::maxtime() {
                return;
            }

            // acceptible becasue we can't lock the DB between doing the wallog.apply and the read (no way to do it right now).
            // Therefore, any new writes between that wallog.apply and the comprehensive "/" read will be logged as error, even though
            // that's perfectly OK for the purposes of the test.
            // if a.1.as_inner().0.get_end_time() != Timestamp::maxtime() && b.1.as_inner().0.get_end_time() == Timestamp::maxtime() {
            //     println!("Conflict acceptible, because of small locking problems");
            // } else {
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
    println!("Start checking");
    let db2 = db!();
    let wallog = db.wallog.clone();
    let time = wallog.apply(&db2).unwrap() - Timestamp::from(1);
    println!("Done applied");
    check_func1(db, &db2, time)
}
