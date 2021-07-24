use std::collections::{BTreeMap, VecDeque, HashMap};
use std::borrow::Borrow;
use std::ops::{Add, Bound, Deref};
use std::str::FromStr;
use serde_json::Value as JSONValue;
use std::cmp::Ordering;
use std::iter::{FromIterator};
use std::fmt::{Display, Formatter, Write};

use slab::Slab;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ObjectPath(String);


#[derive(Debug)]
enum PrimitiveValue {
    String(String),
    Number(f64),
    Boolean(bool),
}

impl Display for ObjectPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Display for PrimitiveValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(str) => f.write_str(&str.to_string()),
            Self::Number(num) => f.write_str(&num.to_string()),
            Self::Boolean(bool) => f.write_str(&bool.to_string())
        }
    }
}

enum PrimValueOrOther {
    PrimitiveValue(PrimitiveValue),
    Other(JSONValue),
}


impl ObjectPath {
    pub fn new(str: &str) -> Self {
        return Self(str.to_string());
    }

    pub fn concat<T: Borrow<str>>(&self, other: T) -> Self {
        let concat = format!("{}/{}", self.0, other.borrow());
        return Self::new(&concat);
    }
}

impl Default for ObjectPath {
    fn default() -> Self {
        Self("/user".to_string())
    }
}

impl Into<String> for ObjectPath {
    fn into(self) -> String {
        self.0
    }
}


fn json_to_map(json: JSONValue) -> Vec<(ObjectPath, PrimitiveValue)> {
    let mut res = Vec::new();

    let mut obj_queue: VecDeque<(JSONValue, ObjectPath)> = VecDeque::from_iter([(json, Default::default())]);

    while !obj_queue.is_empty() {
        let (obj, prefix) = obj_queue.pop_back().unwrap();

        let primvalue = match obj {
            JSONValue::Number(n) => PrimValueOrOther::PrimitiveValue(PrimitiveValue::Number(n.as_f64().unwrap())),
            JSONValue::String(str) => PrimValueOrOther::PrimitiveValue(PrimitiveValue::String(str)),
            JSONValue::Bool(boolean) => PrimValueOrOther::PrimitiveValue(PrimitiveValue::Boolean(boolean)),
            JSONValue::Null => PrimValueOrOther::PrimitiveValue(PrimitiveValue::String("null".to_string())),
            _ => PrimValueOrOther::Other(obj)
        };

        match primvalue {
            PrimValueOrOther::PrimitiveValue(val) => {
                res.push((prefix, val));
            }

            PrimValueOrOther::Other(val) => {
                let additions: VecDeque<(JSONValue, ObjectPath)> = match val {
                    JSONValue::Array(vec) => {
                        let length = (JSONValue::Number(serde_json::Number::from(vec.len())), prefix.concat("length"));
                        let mut elems: VecDeque<_> = vec.into_iter().enumerate().map(|(index, elem)| (elem, prefix.concat(index.to_string()))).collect();

                        elems.push_back(length);
                        elems
                    }
                    JSONValue::Object(obj) => {
                        obj.into_iter().map(|(key, value)| (value, prefix.concat(key))).collect()
                    }
                    _ => unreachable!()
                };

                obj_queue.extend(additions.into_iter());
            }
        };
    };
    return res;
}

struct DisplayableVec<T> {
    pub vec: Vec<T>,
    pub displayer: Box<dyn Fn(&T, &mut Formatter<'_>) -> std::fmt::Result>,
}


struct DisplayablePair<T: Display, T1: Display> {
    first: T,
    second: T1,
}

impl<T> Display for DisplayableVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("[")?;

        let mut has_sep = false;
        self.vec.iter().for_each(|elem: &T| {
            if !has_sep {
                has_sep = true;
            } else {
                f.write_str(",\n");
            }

            (self.displayer)(elem, f);
        });
        f.write_str("]")
    }
}


fn displayer(a: &(ObjectPath, PrimitiveValue), f: &mut Formatter<'_>) -> std::fmt::Result {
    f.write_fmt(format_args!("({}, {})", a.0, a.1))
}


#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub struct Timestamp(u64);

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(match (self.0, other.0) {
            (0, 0) => Ordering::Equal,
            (0, left) => Ordering::Greater,
            (right, 0) => Ordering::Less,
            (left, right) => left.cmp(&right)
        })
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}


use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use crate::WriteIntentStatus::Committed;
use std::cell::RefCell;
use std::rc::Rc;

static MONOTIC_COUNTER: AtomicU64 = AtomicU64::new(1);

impl Timestamp {
    pub fn mintime() -> Self {
        Self(1)
    }
    pub fn maxtime() -> Self {
        Self(0)
    }

    pub fn now() -> Self {
        Self(MONOTIC_COUNTER.fetch_add(1, AtomicOrdering::Relaxed))
    }
}

impl From<u64> for Timestamp {
    fn from(a: u64) -> Self {
        Self(a)
    }
}

struct Transaction();

#[derive(Eq, PartialEq, Hash, Debug)]
struct TransactionRef(u64);

impl Transaction {
    pub fn get_write_intent(&self) -> WriteIntentStatus {
        unimplemented!()
    }
}

impl TransactionRef {
    pub fn to_txn<'a>(&self, map: &'a HashMap<TransactionRef, Transaction>) -> &'a Transaction {
        map.get(self).unwrap()
    }
}

#[derive(PartialEq)]
enum WriteIntentStatus {
    Aborted,
    Pending,
    Committed,
}

#[derive(Debug)]
struct WriteIntent {
    associated_transaction: TransactionRef,
}

struct PrivDbContext {
    pub db: *mut BTreeMap<KeyAndTimestamp, (MVCCMetadata, String)>,
    pub transaction_map: HashMap<TransactionRef, Transaction>,
}

pub type DbContext = Rc<PrivDbContext>;

#[derive(Debug)]
pub struct MVCCMetadata {
    begin_ts: Timestamp,
    end_ts: Timestamp,
    last_read: Timestamp,
    cur_write_intent: Option<WriteIntent>,
}

impl MVCCMetadata {
    pub fn new_default(timestamp: Timestamp) -> Self {
        Self {
            begin_ts: timestamp,
            end_ts: Timestamp::maxtime(),
            last_read: Timestamp::mintime(),
            cur_write_intent: None,
        }
    }

    fn check_write_intents(&self, ctx: &DbContext) -> Result<(), String> {
        match &self.cur_write_intent {
            None => Ok(()),
            Some(wi) => if ctx.transaction_map.get(&wi.associated_transaction).unwrap().get_write_intent() != Committed {
                Err("Write Intent still exists".to_owned())
            } else {
                // TODO: Remove write intent because it was committed.
                Ok(())
            }
        }
    }

    pub fn check_write(&self, ctx: &DbContext, timestamp: Timestamp) -> Result<(), String> {
        self.check_write_intents(ctx)?;

        if timestamp < self.last_read {
            return Err("Timestamp bigger than last read".to_string());
        }

        if timestamp < self.begin_ts || timestamp > self.end_ts {
            return Err("Timestamp not between begin and end".to_string());
        }


        Ok(())
    }

    pub fn get_newer_version(&self) -> Self {
        Self {
            begin_ts: self.end_ts,
            end_ts: Timestamp::maxtime(),
            last_read: self.end_ts,
            cur_write_intent: None,
        }
    }

    pub fn deactivate(&mut self, timestamp: Timestamp) -> Result<(), String> {
        self.end_ts = timestamp;
        self.last_read = self.last_read.max(timestamp);
        assert!(self.begin_ts <= self.end_ts);
        assert!(self.cur_write_intent.is_none());
        Ok(())
    }

    pub fn check_read(&mut self, ctx: &DbContext, timestamp: Timestamp) -> Result<(), String> {
        if timestamp < self.begin_ts || timestamp > self.end_ts {
            return Err("Timestamp not valid".to_string());
        }

        self.check_write_intents(ctx)?;

        self.last_read = self.last_read.max(timestamp);
        Ok(())
    }
}


#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct KeyAndTimestamp(pub ObjectPath, pub Timestamp);

pub type DbType = BTreeMap<KeyAndTimestamp, (MVCCMetadata, String)>;

mod metadata_manager {
    pub use std::collections::{BTreeMap, Bound};
    pub use crate::{KeyAndTimestamp, MVCCMetadata, ObjectPath, Timestamp, DbContext, DbType};
    pub use std::ops::Deref;
    use crate::{WriteIntent, TransactionRef};

    pub unsafe fn update(mut ctx: DbContext, key: &ObjectPath, write_timestamp: Timestamp, new_value: String) -> Result<(), String> {
        let newerversion = if check_has_value(&mut *ctx.db, &key) {
            let (keyandtime, (metadata, value)) = get_last_mvcc_value(&mut *ctx.db, &key);

            metadata.check_write(&ctx, write_timestamp)?;

            metadata.deactivate(write_timestamp);

            let mut newer = metadata.get_newer_version();
            let writeintent = WriteIntent {
                associated_transaction: TransactionRef(0)
            };
            newer.cur_write_intent.insert(writeintent);
            newer
        } else {
            MVCCMetadata::new_default(write_timestamp)
        };
        ctx.db.as_mut().unwrap().insert(KeyAndTimestamp(key.clone(), write_timestamp), (newerversion, new_value));

        Ok(())
    }

    fn check_has_value(db: &DbType, key: &ObjectPath) -> bool {
        let minbound = Bound::Unbounded;
        let maxbound = Bound::Included(KeyAndTimestamp(key.clone(), Timestamp::maxtime()));


        db.range((minbound, maxbound)).next().is_some()
    }

    fn get_last_mvcc_value<'a>(db: &'a mut DbType, key: &ObjectPath) -> (&'a KeyAndTimestamp, &'a mut (MVCCMetadata, String)) {
        let minbound = Bound::Unbounded;
        let maxbound = Bound::Included(KeyAndTimestamp(key.clone(), Timestamp::maxtime()));


        let res = db.range_mut((minbound, maxbound)).last().unwrap();
        assert_eq!(&res.0.0, key);

        return res;
    }

    pub unsafe fn read<'a>(ctx: &'a mut DbContext, key: &ObjectPath, read_time: Timestamp) -> Result<&'a str, String> {
        let (keyandtime, (metadata, str)) = get_last_mvcc_value(&mut *ctx.db, key);

        metadata.check_read(ctx, read_time)?;

        Ok(str)
    }
}


fn test_json() {
    let map = BTreeMap::<ObjectPath, i64>::new();

    let json = serde_json::json!({"nested": {
        "nested_arr": [1, 4, 2, 5, 6, 3, 2]
    }, "nested_2": {
        "test": true,
        "ads": JSONValue::Null,
        "vcxf": [JSONValue::Null, 5, "fdvc"]
    }});

    let res = json_to_map(json);

    let boxedfunc = Box::new(displayer);

    let displayable = DisplayableVec {
        vec: res,
        displayer: boxedfunc,
    };
    println!("Result: {}", displayable);
}


unsafe fn main1() {
    let mut db = BTreeMap::new();
    let dbctx = PrivDbContext {
        db: &mut db as *mut BTreeMap<KeyAndTimestamp, (MVCCMetadata, String)>,
        transaction_map: HashMap::new(),
    };
    let mut ctx = Rc::new(dbctx);
    let key = ObjectPath::new("testkey");
    let value = "testvalue".to_string();

    metadata_manager::update(ctx.clone(), &key, Timestamp::from(5u64), value.clone());


    println!("{:?}\n", db);


    metadata_manager::read(&mut ctx, &key, Timestamp::from(6u64));
    println!("{:?}\n", db);

    metadata_manager::update(ctx.clone(), &key, Timestamp::from(7u64), "value2".to_string());
    println!("{:?}\n", db);


}

fn main() {
    unsafe { main1() }
}
