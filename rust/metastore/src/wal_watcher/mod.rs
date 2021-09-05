use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use wal_apply::apply_wal_txn_checked;

use crate::object_path::ObjectPath;
use crate::rpc_handler::{DatabaseInterface, NetworkResult};
use crate::rwtransaction_wrapper::{LockDataRef, ValueWithMVCC};
use crate::timestamp::Timestamp;
use crate::{DbContext, TypedValue};
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::fs::{File, OpenOptions};

mod serialize_deserialize;
mod test;
mod wal_apply;
pub mod wal_check_consistency;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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
    ops: Vec<Operation<ObjectPath, TypedValue>>,
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

#[allow(clippy::mutex_atomic)]
pub struct ByteBufferWAL {
    buf: RefCell<Vec<u8>>,
    json_lock: Mutex<()>,
    file: Mutex<File>,
    frozen: Mutex<bool>,
}

// impl Clone for ByteBufferWAL {
//     fn clone(&self) -> Self {
//         let _l = self.json_lock.lock().unwrap();
//         // let f = File::ccf
//         Self { buf: self.buf.clone(), json_lock: Mutex::new(()), frozen: Mutex::new(false), file:  }
//     }
// }

impl Display for ByteBufferWAL {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(unsafe { std::str::from_utf8_unchecked(&self.buf.borrow()) })
    }
}

impl ByteBufferWAL {
    pub fn new() -> Self {
        use rand::thread_rng;
        let s = thread_rng().gen::<u16>().to_string();
        let path = "/tmp/wallog-".to_string() + &s;
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .truncate(true)
            .create(true)
            .open(path)
            .unwrap();
        let file = Mutex::new(file);
        Self {
            buf: RefCell::new(Vec::new()),
            json_lock: Mutex::new(()),
            frozen: Mutex::new(false),
            file,
        }
    }
}

impl Write for &ByteBufferWAL {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let l = self.json_lock.lock().unwrap();
        let mut buf = self.buf.borrow_mut();

        let mut file = self.file.lock().unwrap();
        file.write_all(buf.as_slice())?;
        file.flush();
        buf.clear();
        Ok(())
    }
}

impl WalStorer for ByteBufferWAL {
    type K = ObjectPath;
    type V = ValueWithMVCC;
    fn store(&self, waltxn: WalTxn) -> Result<(), String> {
        if *self.frozen.lock().unwrap() {
            return Err("Wal log is currently frozen".to_string());
        }

        {
            let _guard = self.json_lock.lock().unwrap();
            serde_json::to_writer(self, &waltxn).unwrap();
        }
        let mut k = self;
        k.flush();
        Ok(())
    }
    fn raw_data(&self) -> Vec<u8> {
        self.buf.borrow().clone()
    }
}

impl WalLoader for ByteBufferWAL {
    fn load(&self) -> Vec<WalTxn> {
        let buf: Vec<u8> = {
            let _guard = self.json_lock.lock().unwrap();
            let mut filebuf = Vec::new();
            let mut file = self.file.lock().unwrap();
            let prevpos = file.stream_position().unwrap();

            file.seek(SeekFrom::Start(0));
            file.read_to_end(&mut filebuf);

            file.seek(SeekFrom::Start(prevpos));
            filebuf.extend(self.buf.borrow().iter());
            filebuf
        };

        let iter = serde_json::Deserializer::from_reader(buf.as_slice()).into_iter::<WalTxn>();
        let mut vec: Vec<_> = iter.map(|a| a.unwrap()).collect();

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
            apply_wal_txn_checked(elem.clone(), ctx);
        }
        Ok(max_time)
    }
}

impl WalTxn {
    fn log_write(&mut self, k: ObjectPath, v: TypedValue) {
        self.ops.push(Operation::Write(k, v));
    }

    pub fn new(timestamp: Timestamp) -> Self {
        WalTxn {
            ops: vec![],
            timestamp,
        }
    }
}

impl DatabaseInterface for &mut WalTxn {
    fn new_transaction(&self, txn: &LockDataRef) -> NetworkResult<(), String> {
        unreachable!()
    }

    fn serve_read(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
    ) -> NetworkResult<ValueWithMVCC, String> {
        unreachable!()
    }

    fn serve_range_read(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
    ) -> NetworkResult<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        unreachable!()
    }

    fn serve_write(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
        value: TypedValue,
    ) -> NetworkResult<(), String> {
        // safety: we implemented this trait for &mut WalTxn so we can cast.
        let k = unsafe { &mut *(*self as *const WalTxn as *mut WalTxn) };

        k.log_write(key.clone(), value);

        NetworkResult::default()
    }

    fn commit(&self, txn: LockDataRef) -> NetworkResult<(), String> {
        unreachable!()
    }

    fn abort(&self, p0: LockDataRef) -> NetworkResult<(), String> {
        unreachable!()
    }
}
