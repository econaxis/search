use std::fmt::{Display, Formatter};
use std::io::Write;

use serde::de::{MapAccess, Visitor};
use serde::ser::{SerializeStruct, SerializeStructVariant, SerializeTuple};
use serde::{Deserialize, Deserializer, Serialize, Serializer as SS, Serializer};

use wal_apply::apply_wal_txn_checked;

use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::{MVCCMetadata, RWTransactionWrapper};
use crate::rwtransaction_wrapper::ValueWithMVCC;
use crate::timestamp::Timestamp;
use crate::DbContext;
use std::sync::Mutex;
use std::cell::RefCell;
use std::time::Duration;

mod wal_apply;
mod test;

extern crate serde_json;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Operation<K, V> {
    Write(K, V),
    Read(K, V),
}

impl PartialEq for Operation<ObjectPath, ValueWithMVCC> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Operation::Write(_k, _v) => matches!(other, Operation::Write(_k, _v)),
            Operation::Read(_k, _v) => matches!(other, Operation::Read(_k, _v)),
        }
    }
}

impl Serialize for Operation<ObjectPath, ValueWithMVCC> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let converted = match self {
            Operation::Write(k, v) => Operation::Write(CustomSerde(k), CustomSerde(v)),
            Operation::Read(k, v) => Operation::Read(CustomSerde(k), CustomSerde(v)),
        };

        converted.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Operation<ObjectPath, ValueWithMVCC> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        // deserializer.deserialize_struct()
        let converted =
            Operation::<CustomSerde<ObjectPath>, CustomSerde<ValueWithMVCC>>::deserialize(
                deserializer,
            )?;
        Ok(match converted {
            Operation::Write(k, v) => Operation::Write(k.into(), v.into()),
            Operation::Read(k, v) => Operation::Read(k.into(), v.into()),
        })
    }
}

struct CustomSerde<K>(K);

impl Into<ObjectPath> for CustomSerde<ObjectPath> {
    fn into(self) -> ObjectPath {
        self.0
    }
}

impl Into<ValueWithMVCC> for CustomSerde<ValueWithMVCC> {
    fn into(self) -> ValueWithMVCC {
        self.0
    }
}

impl Serialize for CustomSerde<&ObjectPath> {
    fn serialize<SS>(&self, s: SS) -> Result<SS::Ok, SS::Error>
        where
            SS: Serializer,
    {
        s.serialize_newtype_struct("ObjectPath", self.0.as_str())
    }
}

impl Serialize for CustomSerde<&ValueWithMVCC> {
    fn serialize<SS>(&self, s: SS) -> Result<SS::Ok, SS::Error>
        where
            SS: Serializer,
    {
        let mut stct = s.serialize_struct("ValueWithMVCC", 2)?;
        let inner = self.0.as_inner();
        stct.serialize_field("MVCC", &inner.0)?;
        stct.serialize_field("Value", &*inner.1)?;
        stct.end()
    }
}

impl<'de> Deserialize<'de> for CustomSerde<ObjectPath> {
    fn deserialize<D>(deser: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        struct ObjPathVisitor;
        impl<'de> Visitor<'de> for ObjPathVisitor {
            type Value = ObjectPath;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("CustomSerde ObjectPath")
            }

            fn visit_newtype_struct<D>(self, d: D) -> Result<Self::Value, D::Error>
                where
                    D: Deserializer<'de>,
            {
                Ok(ObjectPath::from(String::deserialize(d)?))
            }
        }

        Ok(CustomSerde(deser.deserialize_newtype_struct(
            "ObjectPath",
            ObjPathVisitor,
        )?))
    }
}

impl<'de> Deserialize<'de> for CustomSerde<ValueWithMVCC> {
    fn deserialize<D>(deser: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = ValueWithMVCC;

            fn expecting(&self, _formatter: &mut Formatter) -> std::fmt::Result {
                _formatter.write_str("ValueWithMVCC")
            }

            fn visit_map<A>(self, mut v: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
            {
                let (mvcccheck, mvccvalue) = v.next_entry::<String, MVCCMetadata>()?.unwrap();
                assert!(&mvcccheck == "MVCC");
                let (valuecheck, value) = v.next_entry::<String, String>()?.unwrap();
                assert!(&valuecheck == "Value");
                Ok(ValueWithMVCC::from_tuple(mvccvalue, value))
            }
        }

        Ok(CustomSerde(deser.deserialize_struct(
            "ValueWithMVCC",
            &["MVCC", "Value"],
            ValueVisitor,
        )?))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalTxn {
    ops: Vec<Operation<ObjectPath, ValueWithMVCC>>,
    timestamp: Timestamp,
}

impl PartialEq for WalTxn {
    fn eq(&self, other: &Self) -> bool {
        self.ops
            .iter()
            .zip(&other.ops)
            .all(|(my, other)| my == other)
    }
}

#[derive(Default)]
pub struct ByteBufferWAL {
    buf: RefCell<Vec<u8>>,
    json_lock: Mutex<()>,
}

impl Clone for ByteBufferWAL {
    fn clone(&self) -> Self {
        let l = self.json_lock.lock().unwrap();
        Self { buf: self.buf.clone(), json_lock: Mutex::new(()) }
    }
}

impl Display for ByteBufferWAL {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(unsafe { std::str::from_utf8_unchecked(&self.buf.borrow()) })
    }
}

impl ByteBufferWAL {
    pub fn new() -> Self {
        Self { buf: RefCell::new(Vec::new()), json_lock: Mutex::new(()) }
    }
    pub fn print(&self) -> Vec<u8> {
        println!("{}", std::str::from_utf8(&self.buf.borrow()).unwrap());
        self.buf.borrow().clone()
    }
}

impl Write for &ByteBufferWAL {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        unimplemented!()
    }
}


impl WalStorer for ByteBufferWAL {
    type K = ObjectPath;
    type V = ValueWithMVCC;
    fn store(&self, waltxn: WalTxn) -> Result<(), String> {
        let _guard = self.json_lock.lock().unwrap();
        serde_json::to_writer(self, &waltxn).unwrap();
        Ok(())
    }
    fn raw_data(&self) -> Vec<u8> {
        self.buf.borrow().clone()
    }
}

impl WalLoader for ByteBufferWAL {
    fn load(&self) -> Vec<WalTxn> {
        let buf = {
            let _guard = self.json_lock.lock().unwrap();
            self.buf.borrow().clone()
        };

        let iter = serde_json::Deserializer::from_reader(buf.as_slice()).into_iter::<WalTxn>();
        let mut vec: Vec<_> = iter.map(|a| {
            let a = a.unwrap();
            a
        }).collect();

        vec.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        vec
    }
}

pub trait WalStorer {
    type K;
    type V;
    fn store(&self, waltxn: WalTxn) -> Result<(), String>;

    fn raw_data(&self) -> Vec<u8>;
}

pub trait WalLoader: WalStorer {
    fn load(&self) -> Vec<WalTxn>;

    fn apply(&self, ctx: &DbContext) -> Result<Timestamp, String> {
        let total = self.load();
        let mut max_time = Timestamp::mintime();
        for elem in &total {
            max_time = max_time.max(elem.timestamp);
            if let Err(e) = apply_wal_txn_checked(elem.clone(), ctx) {
                println!("WAL Log error! {}", std::str::from_utf8(&*self.raw_data()).unwrap());
                println!("Current WAL: {:?}", elem);
                println!("{}", e);
                panic!()
            }
        }
        Ok(max_time)
    }
}

impl WalTxn {
    pub fn log_read(&mut self, k: ObjectPath, v: ValueWithMVCC) {
        self.ops.push(Operation::Read(k, v));
    }

    pub fn log_write(&mut self, k: ObjectPath, v: ValueWithMVCC) {
        self.ops.push(Operation::Write(k, v));
    }

    pub fn new(timestamp: Timestamp) -> Self {
        WalTxn {
            ops: vec![],
            timestamp,
        }
    }
}

fn check_func(db: &DbContext) -> Result<bool, String> {
    println!("Start checking");
    let db2 = db!();
    let wallog = db.wallog.clone();
    let time = wallog.apply(&db2).unwrap() - Timestamp::from(1);
    println!("Done applied");
    let mut txn2 = RWTransactionWrapper::new_with_time(&db2, time);
    let ret2 = txn2.read_range_owned(&"/".into())?;
    let mut txn = RWTransactionWrapper::new_with_time(&db, time);
    let mut ret: Result<_, _> = Err("a".into());
    while !ret.is_ok() {
        println!("check {}", ret.unwrap_err());
        std::thread::sleep(Duration::from_millis(1000));
        ret = txn.read_range_owned(&"/".into());
    }
    let ret = ret.unwrap();
    println!("Done reading ranges");

    ret.iter().zip(ret2.iter()).for_each(|(a, b)| {
        if !(a.0 == b.0 && a.1.as_inner().1 == b.1.as_inner().1 &&
            a.1.as_inner().1.parse::<u64>().unwrap() % 10 == 0) {

            // acceptible becasue we can't lock the DB between doing the wallog.apply and the read (no way to do it right now).
            // Therefore, any new writes between that wallog.apply and the comprehensive "/" read will be logged as error, even though
            // that's perfectly OK for the purposes of the test.
            // if a.1.as_inner().0.get_end_time() != Timestamp::maxtime() && b.1.as_inner().0.get_end_time() == Timestamp::maxtime() {
            //     println!("Conflict acceptible, because of small locking problems");
            // } else {
            print!("Time: {}\n", txn.get_txn().id);
            println!("Non matching {:?}", a);
            println!("Non matching {:?}", b);
            wallog.print();
            panic!("Split brain between WAL log and the main DB. applying WAL log failed");
        };
    });
    println!("End checking");

    Ok(true)
}


pub mod tests {
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::{LockDataRef, ValueWithMVCC, RWTransactionWrapper};

    use crate::timestamp::Timestamp;
    use crate::wal_watcher::{WalTxn, ByteBufferWAL, WalStorer, WalLoader, check_func};
    use crate::rwtransaction_wrapper::auto_commit;
    use rand::seq::SliceRandom;
    use rand::{Rng, thread_rng, RngCore};
    use std::borrow::Cow;
    use rand::rngs::ThreadRng;
    use crossbeam::scope;
    use std::time::Duration;
    use std::sync::atomic::{AtomicBool, Ordering, AtomicU64};
    use crate::DbContext;
    use std::sync::atomic::Ordering::SeqCst;

    static COM: AtomicU64 = AtomicU64::new(0);
    static FAIL: AtomicU64 = AtomicU64::new(0);

    // #[test]
    pub fn test1() {
        let keys: Vec<_> = (0..5).map(|a| a.to_string()).collect();
        let db = db!();

        let process = |mut rng: Box<dyn RngCore>, mut iters: u64| while iters > 0 {
            if rng.gen_bool(0.01) {
                println!("rem: {} {}/{}", iters, COM.load(SeqCst), FAIL.load(SeqCst));
            }

            let mut txn = RWTransactionWrapper::new(&db);

            let key = keys.choose(&mut *rng).unwrap();
            let mut all_good = true;
            for _ in 0..10 {
                std::thread::sleep(Duration::from_millis(10));
                let res = txn.read(key.into()).and_then(|str| {
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
                COM.fetch_add(1, Ordering::SeqCst);

                txn.commit();
            } else {
                FAIL.fetch_add(1, Ordering::SeqCst);
                txn.abort();
            }
        };


        let state = AtomicBool::new(false);
        scope(|s| {
            let threads: Vec<_> = std::iter::repeat_with(|| {
                s.spawn(|_| {
                    let rng = Box::new(thread_rng());
                    process(rng, 1000);
                })
            }).take(16).collect();

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

        // println!("final state: {}", db.db.printdb());
    }
}