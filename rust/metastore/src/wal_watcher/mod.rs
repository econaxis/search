use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use wal_apply::apply_wal_txn_checked;

use crate::DbContext;
use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::ValueWithMVCC;
use crate::timestamp::Timestamp;

mod wal_apply;
mod test;
mod serialize_deserialize;
pub mod wal_check_consistency;

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
    frozen: Mutex<bool>,
}



impl Clone for ByteBufferWAL {
    fn clone(&self) -> Self {
        let _l = self.json_lock.lock().unwrap();
        Self { buf: self.buf.clone(), json_lock: Mutex::new(()), frozen: Mutex::new(false) }
    }
}

impl Display for ByteBufferWAL {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(unsafe { std::str::from_utf8_unchecked(&self.buf.borrow()) })
    }
}

impl ByteBufferWAL {
    pub fn new() -> Self {
        Self { buf: RefCell::new(Vec::new()), json_lock: Mutex::new(()), frozen: Mutex::new(false) }
    }
    pub fn print(&self) -> Vec<u8> {
        println!("{}", std::str::from_utf8(&self.buf.borrow()).unwrap());
        self.buf.borrow().clone()
    }
    pub fn freeze(&self) {
        let mut b = self.frozen.lock().unwrap();
        *b = true;
    }
    pub fn unfreeze(&self) {
        let mut b = self.frozen.lock().unwrap();
        *b = false;
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
        if *self.frozen.lock().unwrap() { return Err("Wal log is currently frozen".to_string()); }

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
        // println!("{:?}", self.ops);
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


