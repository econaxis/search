// #[cfg(test)]
pub mod tests {
    use crate::{create_empty_context, DbContext};
    use crate::rwtransaction_wrapper::RWTransactionWrapper;

    use crate::object_path::ObjectPath;
    use std::sync::Arc;
    use std::borrow::{Cow};
    use std::collections::HashSet;
    use rand::prelude::*;
    use std::time::{Duration, Instant};

    #[test]
    pub fn run() {
        unique_set_insertion_test();
    }

    pub fn unique_set_insertion_test() {
        static BADVALUE: u64 = 99999999999;

        let ctx1 = Arc::new(create_empty_context());

        let mut txn = RWTransactionWrapper::new(&*ctx1);
        txn.write(&ObjectPath::from(format!("/test/{}", "1")), "1".into()).unwrap();
        txn.commit();

        let clos = |ctx: Arc<DbContext>| {
            let ctx: &DbContext = &ctx;
            let mut rng = thread_rng();

            let mut i = 0;
            let time = Instant::now();
            std::thread::sleep(Duration::from_millis(rng.gen::<u64>() % 500));

            while i < 200 {
                let mut txn = RWTransactionWrapper::new(&ctx);
                let range = txn.read_range_owned(&ObjectPath::new("/test/"));

                if let Ok(range) = range {
                    for x in range {
                        assert!(x.1.into_inner().1.parse::<u64>().unwrap() < BADVALUE);
                    }
                    let max = time.elapsed().as_micros() % 2;
                    let maxnumstr = max.to_string();

                    let first = txn.write(&ObjectPath::from(format!("/test/{}", maxnumstr)), Cow::from(format!("{}{}", BADVALUE.to_string(), txn.get_txn().id.to_string())));
                    std::thread::sleep(Duration::from_millis(50 + rng.gen::<u64>() % 200));

                    let first = first.and(txn.write(&ObjectPath::from(format!("/test/{}", maxnumstr)), Cow::from(format!("{}{}", BADVALUE.to_string(), txn.get_txn().id.to_string()))));
                    std::thread::sleep(Duration::from_millis(50 + rng.gen::<u64>() % 200));


                    let second = txn.write(&ObjectPath::from(format!("/test/{}", maxnumstr)), maxnumstr.into());

                    let total = first.and(second);
                    if total.is_ok() && rng.gen_bool(0.9) {
                        txn.commit();
                    } else {
                        txn.abort();
                        println!("aborted {:?} {:?}", total, txn.get_txn().timestamp);
                    };
                    i += 1;
                } else {
                    // println!("transaction conflict {}, {:?}", range.unwrap_err(), txn.get_txn().timestamp);
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        };

        let mut joins = Vec::new();

        for _ in 0..16 {
            let ctx1 = ctx1.clone();
            let t1 = std::thread::spawn(move || {
                clos(ctx1);
            });
            joins.push(t1);
        }

        for x in joins {
            x.join().unwrap();
        };

        let mut txn = RWTransactionWrapper::new(&ctx1);
        let mut uniq = HashSet::new();
        assert!(txn.read_range_owned(&ObjectPath::new("/test/")).unwrap().iter().all(|a| {
            let val = a.1.as_inner().1.parse::<u64>().unwrap();
            assert!(val < BADVALUE);
            uniq.insert(val)
        }));

        //println!("{}", ctx1.db.printdb());
    }
}