use std::fmt::Formatter;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeStruct;

use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::{MVCCMetadata, ValueWithMVCC};
use crate::wal_watcher::Operation;

pub struct CustomSerde<K>(K);

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