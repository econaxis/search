// wal.log_read()
// wal.log_write()
// wal.done() -> flushes the wal entry to the WAL.

// WAL.flush_log()
// WAL.new_transaction()
extern crate serde_json;

use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::{ValueWithMVCC, MVCCMetadata};
use crate::timestamp::Timestamp;
use std::io::{Write, Read};
use serde_json::{Serializer};
use serde::{Serialize, Serializer as SS, Deserialize, Deserializer};
use serde::de::{Visitor, MapAccess};
use std::borrow::Borrow;
use serde::ser::{SerializeTuple, SerializeStruct};
use serde_json::ser::PrettyFormatter;
use std::fmt::Formatter;

pub struct WalTxn<K = ObjectPath, V = ValueWithMVCC> {
    reads: Vec<K>,
    writes: Vec<(K, V)>,
    timestamp: Timestamp,
}

#[repr(transparent)]
struct CustomKey(ObjectPath);

#[repr(transparent)]
struct CustomValue((CustomKey, ValueWithMVCC));

impl Serialize for CustomValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: SS {
        let mut tup = serializer.serialize_tuple(3)?;
        tup.serialize_element(&self.0.0)?;
        tup.serialize_element(&self.0.1.1)?;
        tup.serialize_element(&self.0.1.0)?;
        tup.end()
    }
}

impl Serialize for CustomKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: SS {
        serializer.serialize_str(&self.0.as_cow_str().borrow())
    }
}

trait TransparentRepr<T> {}

impl TransparentRepr<ObjectPath> for CustomKey {}
impl TransparentRepr<(ObjectPath, ValueWithMVCC)> for CustomValue {}


fn transmute_vec<From, To: TransparentRepr<From>>(from: &[From]) -> &[To] {
    let fromptr = from.as_ptr();
    unsafe { std::slice::from_raw_parts(fromptr as *const To, from.len()) }
}

fn transmute_vec_backwards<From: TransparentRepr<To>, To>(from: &[From]) -> &[To] {
    let fromptr = from.as_ptr();
    unsafe { std::slice::from_raw_parts(fromptr as *const To, from.len()) }
}

#[derive(Default)]
struct WALStorer {
    buf: Vec<u8>,
}

impl Write for &mut WALStorer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
        // todo!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

fn my_serialize<'a, S: SS>(s: S, reads: &[impl Serialize], writes: &[impl Serialize]) {
    let mut tup = s.serialize_struct("Entry", 2).unwrap();
    tup.serialize_field("read", reads);
    tup.serialize_field("write", writes);
    tup.end();
}

fn my_deserialize<'de, D: Deserializer<'de>>(d: D) -> <EntryVisitor as Visitor<'de>>::Value {
    d.deserialize_map(EntryVisitor).unwrap()
}

struct EntryVisitor;

impl<'de> Visitor<'de> for EntryVisitor {
    type Value = (Vec<CustomKey>, Vec<CustomValue>);

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        todo!()
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error> where A: MapAccess<'de> {
        let (read, readset): (&str, Vec<&str>) = map.next_entry().unwrap().unwrap();
        let (write, writeset): (&str, Vec<(&str, &str, MVCCMetadata)>) = map.next_entry().unwrap().unwrap();

        assert_eq!(read, "read");
        assert_eq!(write, "write");

        let readset = readset.iter().map(|key| CustomKey(ObjectPath::new(key))).collect();
        let writeset = writeset.iter().map(|(key, value, mvcc)| {
            CustomValue((
                CustomKey(ObjectPath::new(key)),
                ValueWithMVCC(
                    mvcc.clone(), value.to_string())))
        }).collect();

        Ok((readset, writeset))
    }
}

impl WALStorer {
    fn store_committed_txn(&mut self, reads: &Vec<ObjectPath>, writes: &Vec<(ObjectPath, ValueWithMVCC)>) -> Result<(), String> {
        let reads: &[CustomKey] = transmute_vec(&reads);
        let writes: &[CustomValue] = transmute_vec(&writes);

        let mut ser = serde_json::Serializer::<&mut Self, _>::pretty(&mut *self);
        my_serialize(&mut ser, reads, writes);

        Ok(())
    }
    // fn apply_readset(reads: Vec<Self::K>) -> Result<(), String> {
    //     todo!()
    // }
    // fn apply_writeset(writes: Vec<(Self::K, Self::V)>) -> Result<(), String> {
    //     todo!()
    // }

    fn print(&self) -> Vec<u8> {
        println!("{}", std::str::from_utf8(&self.buf).unwrap());
        self.buf.clone()
    }
}


impl<K, V> WalTxn<K, V> {
    pub fn log_read(&mut self, k: K) {
        self.reads.push(k);
    }

    pub fn log_write(&mut self, k: K, v: V) {
        self.writes.push((k, v));
    }

    pub fn done(self) {}
}

// pub fn create_wal_txn()

mod tests {
    use crate::wal_watcher::{WALStorer, my_deserialize};
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::{ValueWithMVCC, MVCCMetadata};
    use crate::timestamp::Timestamp;

    #[test]
    fn wal_serialize_deserialize() {
        let mut a = WALStorer::default();
        let reads = vec![ObjectPath::new("read1"), ObjectPath::new("read2"), ObjectPath::new("read3")];
        let writes = vec![
            (ObjectPath::new("write1"), ValueWithMVCC(MVCCMetadata::new_default(Timestamp::from(5)), "fdsavcx".to_string())),
            (ObjectPath::new("write2"), ValueWithMVCC(MVCCMetadata::new_default(Timestamp::from(55)), "ffdsavcdsavcx".to_string())),
            (ObjectPath::new("write3"), ValueWithMVCC(MVCCMetadata::new_default(Timestamp::from(555)), "fds42tavcx".to_string()))];

        a.store_committed_txn(&reads, &writes);
        let output = a.print();
        let mut deser = serde_json::Deserializer::from_slice(output.as_slice());
        let (reads1, writes1) = my_deserialize(&mut deser);

        let reads1 = super::transmute_vec_backwards(&reads1);
        let writes1 = super::transmute_vec_backwards(&writes1);

        assert_eq!(reads1, reads);
        writes1.iter().zip(writes.iter()).for_each(|(one, two)| {
            assert_eq!(one.0, two.0);
            let ValueWithMVCC(metaone, strone) = &one.1;
            let ValueWithMVCC(metatwo, strtwo) = &two.1;

            assert!(metaone.check_matching_timestamps(&metatwo));
            assert_eq!(strtwo, strone);

        });
    }
}