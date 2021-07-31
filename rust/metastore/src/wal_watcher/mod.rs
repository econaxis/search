use std::fmt::{Display, Formatter};
use std::io::Write;

use serde::{Deserialize, Deserializer, Serialize, Serializer as SS, Serializer};
use serde::de::{MapAccess, Visitor};
use serde::ser::{SerializeStruct, SerializeStructVariant, SerializeTuple};

use wal_apply::apply_wal_txn_checked;

use crate::DbContext;
use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::ValueWithMVCC;
use crate::rwtransaction_wrapper::MVCCMetadata;
use crate::timestamp::Timestamp;

mod wal_apply;

extern crate serde_json;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Operation<K, V> {
    Write(K, V),
    Read(K),
}

impl PartialEq for Operation<ObjectPath, ValueWithMVCC> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Operation::Write(_k, _v) => matches!(other, Operation::Write(_k, _v)),
            Operation::Read(_k) => matches!(other, Operation::Read(_k)),
        }
    }
}

impl Serialize for Operation<ObjectPath, ValueWithMVCC> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let converted = match self {
            Operation::Write(k, v) => Operation::Write(CustomSerde(k), CustomSerde(v)),
            Operation::Read(k) => Operation::Read(CustomSerde(k))
        };

        converted.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Operation<ObjectPath, ValueWithMVCC> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        // deserializer.deserialize_struct()
        let converted = Operation::<CustomSerde<ObjectPath>, CustomSerde<ValueWithMVCC>>::deserialize(deserializer)?;
        Ok(match converted {
            Operation::Write(K, V) => Operation::Write(K.into(), V.into()),
            Operation::Read(K) => Operation::Read(K.into())
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
    fn serialize<SS>(&self, s: SS) -> Result<SS::Ok, SS::Error> where SS: Serializer {
        s.serialize_newtype_struct("ObjectPath", self.0.as_str())
    }
}

impl Serialize for CustomSerde<&ValueWithMVCC> {
    fn serialize<SS>(&self, s: SS) -> Result<SS::Ok, SS::Error> where SS: Serializer {
        let mut stct = s.serialize_struct("ValueWithMVCC", 2)?;
        let inner = self.0.as_inner();
        stct.serialize_field("MVCC", &inner.0)?;
        stct.serialize_field("Value", &inner.1)?;
        stct.end()
    }
}

impl<'de> Deserialize<'de> for CustomSerde<ObjectPath> {
    fn deserialize<D>(deser: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        struct ObjPathVisitor;
        impl<'de> Visitor<'de> for ObjPathVisitor {
            type Value = ObjectPath;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("CustomSerde ObjectPath")
            }

            fn visit_newtype_struct<D>(self, d: D) -> Result<Self::Value, D::Error> where D: Deserializer<'de> {
                Ok(ObjectPath::from(String::deserialize(d)?))
            }
        }

        Ok(CustomSerde(deser.deserialize_newtype_struct("ObjectPath", ObjPathVisitor)?))
    }
}

impl<'de> Deserialize<'de> for CustomSerde<ValueWithMVCC> {
    fn deserialize<D>(deser: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = ValueWithMVCC;

            fn expecting(&self, _formatter: &mut Formatter) -> std::fmt::Result {
                todo!()
            }

            fn visit_map<A>(self, mut v: A) -> Result<Self::Value, A::Error> where A: MapAccess<'de> {
                let (mvcccheck, mvccvalue) = v.next_entry::<String, MVCCMetadata>()?.unwrap();
                assert!(&mvcccheck == "MVCC");
                let (valuecheck, value) = v.next_entry::<String, String>()?.unwrap();
                assert!(&valuecheck == "Value");
                Ok(ValueWithMVCC::from_tuple(mvccvalue, value))
            }
        }

        Ok(CustomSerde(deser.deserialize_struct("ValueWithMVCC", &["MVCC", "Value"], ValueVisitor)?))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalTxn {
    ops: Vec<Operation<ObjectPath, ValueWithMVCC>>,
    timestamp: Timestamp,
}



impl PartialEq for WalTxn {
    fn eq(&self, other: &Self) -> bool {
        self.ops.iter().zip(&other.ops).all(|(my, other)| {
            my == other
        })
    }
}


#[derive(Default)]
pub struct ByteBufferWAL {
    buf: Vec<u8>,
}

impl Display for ByteBufferWAL {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(unsafe { std::str::from_utf8_unchecked(&self.buf) })
    }
}

impl ByteBufferWAL {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }
}

impl Write for &mut ByteBufferWAL {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
        // todo!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

impl ByteBufferWAL {
    fn print(&self) -> Vec<u8> {
        println!("{}", std::str::from_utf8(&self.buf).unwrap());
        self.buf.clone()
    }
}

impl WalStorer for ByteBufferWAL {
    type K = ObjectPath;
    type V = ValueWithMVCC;
    fn store(&mut self, waltxn: WalTxn) -> Result<(), String> {
        serde_json::to_writer_pretty(self, &waltxn).unwrap();
        Ok(())
    }
}

impl WalLoader for ByteBufferWAL {
    fn load<'a>(&'a self) -> Box<dyn Iterator<Item=serde_json::Result<WalTxn>> + 'a> {
        let iter = serde_json::Deserializer::from_reader(self.buf.as_slice()).into_iter::<WalTxn>();
        Box::new(iter)
    }
}

pub trait WalStorer {
    type K;
    type V;
    fn store(&mut self, waltxn: WalTxn) -> Result<(), String>;
}

pub trait WalLoader: WalStorer {
    fn load<'a>(&'a self) -> Box<dyn Iterator<Item=serde_json::Result<WalTxn>> + 'a>;

    fn apply(&self, ctx: &DbContext) -> Result<(), String> {
        self.load().for_each(|elem| {
            apply_wal_txn_checked(elem.unwrap(), ctx);
        });
        Ok(())
    }
}


impl WalTxn {
    pub fn log_read(&mut self, k: ObjectPath) {
        self.ops.push(Operation::Read(k));
    }

    pub fn log_write(&mut self, k: ObjectPath, v: ValueWithMVCC) {
        self.ops.push(Operation::Write(k, v));
    }

    pub fn new(timestamp: Timestamp) -> Self {
        WalTxn { ops: vec![], timestamp }
    }
}


mod tests {
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::{ValueWithMVCC, LockDataRef};
    
    use crate::timestamp::Timestamp;
    use crate::wal_watcher::WalTxn;
    

    impl Arbitrary for ArbWalTxn {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut txn = WalTxn::new(Timestamp::now());
            let writes: Vec<(String, String, u64, u64)> = Arbitrary::arbitrary(g);
            let writes: Vec<_> = writes.into_iter().map(|mut elem| {
                elem.0.push('/');
                (ObjectPath::from(elem.0), ValueWithMVCC::new(LockDataRef {
                    id: 0,
                    timestamp: Timestamp::now()
                }, elem.1))
            }).collect();

            writes.into_iter().for_each(|elem| {
                txn.log_write(elem.0.clone(), elem.1);

                if u8::arbitrary(g) % 10 == 0 {
                    txn.log_read(elem.0)
                }
            });
            ArbWalTxn(txn)
        }
    }

    #[derive(Clone, Debug)]
    struct ArbWalTxn(WalTxn);

    #[quickcheck]
    fn wal_serialize_deserialize(ArbWalTxn(txn): ArbWalTxn) {
        let (mut res, mut res1) = (Vec::<u8>::new(), Vec::<u8>::new());


        serde_json::to_writer(&mut res, &txn);

        let txn1: WalTxn = serde_json::from_reader(&*res).unwrap();
        serde_json::to_writer(&mut res1, &txn1);
        assert_eq!(res, res1);
    }

    #[test]
    fn test1() {
        // let mut a = ByteBufferWAL::new();
        // a.store(random_txn());
        // a.store(random_txn());
        // a.store(random_txn());
        //
        // a.load().for_each(|a| {
        //     let writer = serde_json::to_string(&a.unwrap()).unwrap();
        //     println!("{}", writer);
        // });
        // assert_eq!(a.load().count(), 3);
    }
}