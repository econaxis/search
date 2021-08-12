// #[cfg(test)]
pub mod tests {
    use std::borrow::Cow;
    use std::collections::HashSet;
    use std::hash::Hasher;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use rand::prelude::*;

    use crate::db_context::{create_replicated_context, DbContext};
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::{ReplicatedTxn, ValueWithMVCC};
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;

    pub fn run() {
        unique_set_insertion_test();
    }


    pub fn unique_set_insertion_test() {
        static BADVALUE: u64 = 99999999999;

        let ctx1 = Arc::new(create_replicated_context());

        let mut txn = ReplicatedTxn::new(&*ctx1);
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

                let mut txn = ReplicatedTxn::new(&ctx);
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

        let mut txn = ReplicatedTxn::new(&ctx1);
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

    static TIMER: AtomicU64 = AtomicU64::new(1);

    // inspired from jepsen
    // `add` event: finds max of rows currently in the table and adds one
    // `read` event: reads the whole table.
    // also checks for phantom writes
    // #[test]
    pub fn monotonic() {
        use crossbeam::scope;
        fn now() -> String {
            TIMER.fetch_add(1, SeqCst).to_string()
        }

        fn add(ctx: &DbContext) {
            let retry = || {
                let mut txn = ReplicatedTxn::new(ctx);
                let range = txn.read_range_owned(&ObjectPath::new("/test/"))?;
                let len = range.len();
                let max = range.into_iter().map(|x| x.1.into_inner().1.parse::<u64>().unwrap()).max().unwrap_or(0) + 1;
                if (max - 1) != len as u64 {
                    panic!("bad");
                }
                let key = format!("/test/{}", now());
                txn.write(&ObjectPath::from(key.clone()), "invalid value".into())?;

                std::thread::sleep(Duration::from_micros(10));
                let max = max.to_string();


                txn.write(&ObjectPath::from(key), max.into())?;
                txn.commit();

                Ok::<(), String>(())
            };

            let mut retries = 0;
            while let Err(err) = retry() {
                std::thread::sleep(Duration::from_millis(random::<u64>() % 100 * retries));
                retries += 1;
                if retries % 10 == 0 {
                    println!("retry loop {} {}", retries, err);
                }
            }
        }

        fn check(mut a: Vec<(ObjectPath, ValueWithMVCC)>) {
            let mut prevvalue = String::from("-1");
            let mut values_tested = 0;

            a.sort_by_key(|(k, _)| {
                let k = k.as_str().strip_prefix("/test/").unwrap();
                k.parse::<u64>().unwrap()
            });
            for mut elem in &mut a {
                values_tested += 1;
                // assert!(elem.0.as_str() > &prev_time);
                if !(elem.1.get_readable().val.parse::<i64>().unwrap() > prevvalue.parse::<i64>().unwrap()) {
                    println!("{:?}", a);
                    panic!()
                }
                prevvalue = elem.1.get_readable().val.clone();
            }

            if rand::thread_rng().gen_bool(0.2) { println!("passed, tested {} values", values_tested) }
        }

        fn read(ctx: &DbContext) {
            let retry = || -> Result<(), String> {
                let mut txn = ReplicatedTxn::new(ctx);
                let range = txn.read_range_owned(&ObjectPath::new("/test/"))?;
                // let mut r1 = range.into_iter().map(|a| (a.0, a.1.into_inner().1));
                check(range);

                txn.commit();
                Ok(())
            };

            let mut retries = 1;
            while let Err(err) = retry() {
                retries += 1;
                std::thread::sleep(Duration::from_millis(retries * 10));
                if retries % 10 == 0 {
                    println!("retry loop {} err: {}", retries, err);
                }
            }
        }

        let ctx = db!();
        scope(|s| {
            s.spawn(|_| for _ in 0..150 {
                std::thread::sleep(Duration::from_millis(10));
                read(&ctx)
            });
            s.spawn(|_| for _ in 0..50 {
                std::thread::sleep(Duration::from_millis(10));
                add(&ctx)
            });
            s.spawn(|_| for _ in 0..50 {
                std::thread::sleep(Duration::from_millis(10));
                add(&ctx)
            });
            s.spawn(|_| for _ in 0..50 {
                std::thread::sleep(Duration::from_millis(10));
                add(&ctx)
            });
            s.spawn(|_| for _ in 0..50 {
                std::thread::sleep(Duration::from_millis(10));
                add(&ctx)
            });
        });
    }
}

// wal watcher tests
pub mod tests_walwatcher {
    use std::borrow::Cow;
    use std::io::Write;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::atomic::Ordering::{SeqCst, Relaxed};
    use std::time::Duration;

    use crossbeam::scope;
    use rand::{Rng, RngCore, thread_rng};
    use rand::seq::SliceRandom;

    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::ReplicatedTxn;
    use crate::rwtransaction_wrapper::auto_commit;
    use crate::wal_watcher::{WalLoader, WalStorer};
    use crate::wal_watcher::wal_check_consistency::check_func;

    static COM: AtomicU64 = AtomicU64::new(0);
    static FAIL: AtomicU64 = AtomicU64::new(0);


    pub fn test2() {
        // Each thread increments a number up exactly `n` times. Check that final number is `n` * number of threads
        let db = db!("k" = "0");
        let n = 50;

        let process = || {
            let mut iters = 0;
            while iters < n {
                let mut txn = ReplicatedTxn::new(&db);
                let res: Result<(), String> = try {
                    let value = txn.read(&"k".into())?.parse::<u64>().unwrap() + 1;
                    txn.write(&"k".into(), Cow::from("invalid value".to_string()))?;
                    std::thread::sleep(Duration::from_millis(10));
                    txn.write(&"k".into(), value.to_string().into())?;
                };
                if res.is_ok() {
                    txn.commit();
                    iters += 1;
                } else {
                    txn.abort();
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        };

        scope(|s| {
            let mut i = Vec::with_capacity(5);
            for _ in 0..5 {
                i.push(s.spawn(|_| {
                    process();
                }));
            }
        });

        assert_eq!(auto_commit::read(&db, &"k".into()).unwrap().into_inner().1.as_str(), "250");
    }

    pub fn test1() {
        let keys: Vec<_> = (0..10).map(|a| a.to_string()).collect();
        let db = db!();

        let process = |mut rng: Box<dyn RngCore>, mut iters: u64| while iters > 0 {
            if rng.gen_bool(0.001) {
                println!("rem: {} committed/aborted: {}/{}", iters, COM.load(Relaxed), FAIL.load(Relaxed));
            }

            let mut txn = ReplicatedTxn::new(&db);

            let key = keys.choose(&mut *rng).unwrap();
            let key = ObjectPath::new(&key);
            let mut all_good = true;
            for _ in 0..10 {
                // std::thread::sleep(Duration::from_micros(20));
                let res = txn.read(&key).and_then(|str| {
                    let val = str.parse::<u64>().unwrap() + 1;
                    txn.write(&key.as_str().into(), Cow::from(val.to_string()))
                });

                let res = match res {
                    Err(err) => {
                        if err == "Other(\"Read value doesn't exist\")".to_string() {
                            txn.write(&key.as_str().into(), Cow::from("1")).map(|_| ())
                        } else {
                            Err(format!("Txn error {}", err))
                        }
                    }
                    _ => Ok(())
                };
                all_good &= res.is_ok();

                if !all_good {
                    // println!("abort error {}", res.unwrap_err());
                    break;
                }
            }
            if all_good {
                iters -= 1;
                // println!("commit {}", txn.get_txn().id);
                COM.fetch_add(1, Relaxed);

                txn.commit();
            } else {
                FAIL.fetch_add(1, Relaxed);
                txn.abort();
            }
        };


        let state = AtomicBool::new(false);
        scope(|s| {
            let threads: Vec<_> = std::iter::repeat_with(|| {
                s.spawn(|_| {
                    let rng = Box::new(thread_rng());
                    process(rng, 20000);
                })
                // The less threads we take, the higher the performance, which is expected. :(.
            }).take(5).collect();

            let checker = s.spawn(|_| while !state.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(3000));
                check_func(&db).map_err(|err| println!("Check error: {}", err));
            });
            println!("created threads");

            for x in threads {
                x.join().unwrap();
            }

            state.store(true, Ordering::SeqCst);
            checker.join().unwrap();
        }).unwrap();

    }
}
