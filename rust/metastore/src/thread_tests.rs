// #[cfg(test)]
pub mod tests {
    use crate::rwtransaction_wrapper::RWTransactionWrapper;
    use crate::{create_empty_context, DbContext};

    use crate::object_path::ObjectPath;
    use rand::prelude::*;
    use std::borrow::Cow;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    #[ignore]
    pub fn run() {
        unique_set_insertion_test();
    }

    #[test]
    pub fn independent_writes_dont_block() {
        use crate::wal_watcher::check_func;
        // tests that locks only acquired for individual key/value operations, not for the whole transaction
        let db = db!("k" = "v");
        let mut t1 = RWTransactionWrapper::new(&db);
        let mut t2 = RWTransactionWrapper::new(&db);
        t1.write(&"k".into(), "v2".into());
        match t2.write(&"k".into(), "v3".into()) {
            Err(x) => println!("(good) expected error: {}", x),
            _ => panic!("should've errored")
        }
        match t2.read(&"k".into()) {
            Err(x) => println!("(good) expected error: {}", x),
            _ => panic!("should've errored")
        }
        match t2.write(&"k1".into(), "v3".into()) {
            Ok(x) => println!("(good) expected value: {:?}", x),
            _ => panic!("should've been ok")
        }
        t1.commit();
        t2.commit();
        check_func(&db);
    }

    pub fn unique_set_insertion_test() {
        static BADVALUE: u64 = 99999999999;

        let ctx1 = Arc::new(create_empty_context());

        let mut txn = RWTransactionWrapper::new(&*ctx1);
        txn.write(&ObjectPath::from(format!("/test/{}", "1")), "1".into())
            .unwrap();
        txn.commit();

        let clos = |ctx: Arc<DbContext>| {
            let ctx: &DbContext = &ctx;
            let mut rng = thread_rng();

            let mut i = 0;
            std::thread::sleep(Duration::from_millis(rng.gen::<u64>() % 500));

            while i < 500 {
                if rng.gen_bool(0.0002) {
                    println!("{}", ctx.db.printdb());
                }

                let mut txn = RWTransactionWrapper::new(&ctx);
                let range = txn.read_range_owned(&ObjectPath::new("/test/"));

                if let Ok(range) = range {
                    let max = range.into_iter().map(|x| x.1.into_inner().1.parse::<u64>().unwrap()).max().unwrap() + 1;
                    let maxnumstr = max.to_string();

                    let first = txn.write(
                        &ObjectPath::from(format!("/test/{}", maxnumstr)),
                        Cow::from(format!(
                            "{}{}",
                            BADVALUE.to_string(),
                            txn.get_txn().id.to_string()
                        )),
                    );
                    std::thread::sleep(Duration::from_micros(10 + rng.gen::<u64>() % 10000));

                    let first = first.and(txn.write(
                        &ObjectPath::from(format!("/test/{}", maxnumstr)),
                        Cow::from(format!(
                            "{}{}",
                            BADVALUE.to_string(),
                            txn.get_txn().id.to_string()
                        )),
                    ));
                    std::thread::sleep(Duration::from_micros(10 + rng.gen::<u64>() % 10));

                    let second = txn.write(
                        &ObjectPath::from(format!("/test/{}", maxnumstr)),
                        maxnumstr.into(),
                    );

                    let total = first.and(second);
                    if total.is_ok() && rng.gen_bool(0.8) {
                        // println!("committed {:?}", txn.get_txn().timestamp);
                        txn.commit();
                        i += 1;
                    } else {
                        txn.abort();
                        // println!("aborted {:?} {:?}", total, txn.get_txn().timestamp);
                    };
                } else {
                    // println!("transaction conflict {}, {:?}", range.unwrap_err(), txn.get_txn().timestamp);
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        };

        let mut joins = Vec::new();

        for _ in 0..8 {
            let ctx1 = ctx1.clone();
            let t1 = std::thread::spawn(move || {
                clos(ctx1);
            });
            joins.push(t1);
        }

        for x in joins {
            x.join().unwrap();
        }

        let mut txn = RWTransactionWrapper::new(&ctx1);
        let mut uniq = HashSet::new();
        assert!(txn
            .read_range_owned(&ObjectPath::new("/test/"))
            .unwrap()
            .iter()
            .all(|a| {
                let val = a.1.as_inner().1.parse::<u64>().unwrap();
                assert!(val < BADVALUE);
                uniq.insert(val)
            }));

    }



}
