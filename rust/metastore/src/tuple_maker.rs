use crate::{ObjectPath, TypedValue, ReplicatedTxn, DbContext};
use crate::rwtransaction_wrapper::ValueWithMVCC;
use std::collections::{HashMap, HashSet};
use std::iter::{Peekable, FromIterator};
use crate::parsing::{SelectQuery, ColumnExpr};
use serde_json::Value;

#[derive(Debug)]
struct Tuple(String, HashMap<String, TypedValue>);

enum ExtractValue {
    All,
    List(HashSet<String>),
}

impl From<Vec<ColumnExpr>> for ExtractValue  {
    fn from(col: Vec<ColumnExpr>) -> Self {
        let cols: HashSet<_> = col.into_iter().map(|a| match a {
            ColumnExpr::String(a) => a,
            _ => unimplemented!()
        }).collect();

        ExtractValue::List(cols)
    }
}

fn get_latter<'a>(a: &'a ObjectPath, b: &'_ ObjectPath) -> &'a str {
    assert!(a.as_str().starts_with(b.as_str()));
    let blen = b.as_str().len();
    let a = &a.as_str()[blen..];
    let next_slash = a.find('/').unwrap();
    &a[0..next_slash]
}

#[test]
fn test_get_latter() {
    let b = "/test/a/".into();
    let a = "/test/a/test/4".into();
    assert_eq!(get_latter(&a, &b), "test");
}

impl ExtractValue {
    fn new_list(l: &[&str]) -> Self {
        let s = HashSet::from_iter(l.iter().map(|a| a.to_string()));
        Self::List(s)
    }
    fn check<'a>(&self, top_level: &ObjectPath, key: &'a ObjectPath) -> Option<&'a str> {
        let t = top_level.as_str().len();
        let subtraction = &key.as_str()[t..];
        let first_match = subtraction.find('/').unwrap() + 1;
        let second_match = first_match.max(subtraction.len() - 1);

        let str = &subtraction[first_match..second_match];

        match self {
            Self::All if !str.is_empty() => Some(str),
            Self::List(set) if set.contains(str) =>  Some(str),
            _ => None
        }
    }
}

fn consume_single_tuple<I: Iterator<Item=(ObjectPath, ValueWithMVCC)>>(iter: &mut Peekable<I>, top_level: &ObjectPath, extract_values: &ExtractValue) -> Tuple {
    let mut tup = None;
    loop {
        let v = iter.peek();
        let v = match v {
            Some(x) => x,
            None => break
        };

        assert!(v.0.as_str().ends_with('/'));


        if tup.is_none() {
            tup = Some(Tuple(get_latter(&v.0, top_level).to_string(), HashMap::new()));
        }
        let tup = tup.as_mut().unwrap();

        if get_latter(&v.0, top_level) != tup.0 {
            break;
        }

        if let Some(str) = extract_values.check(top_level, &v.0) {
            let str = str.to_string();
            let v1 = iter.next().unwrap();
            tup.1.insert(str, v1.1.into_inner().1);
        } else {
            iter.next();
            continue;
        }
    };
    tup.unwrap()
}

#[test]
fn test2() {
    let db = db!("/test/and/and1/" = 1f64, "/test/bat/" = 2f64, "/test/cat/" = 2f64,"/test/dog/" = 2f64,"/zther/a/" = "3");
    let mut txn = ReplicatedTxn::new(&db);
    let ret = txn.read_range_owned(&"/".into()).unwrap();

    let mut iter = ret.into_iter().peekable();
    dbg!(consume_single_tuple(&mut iter, &"/".into(), &ExtractValue::new_list(&["bat", "and/and1", "dog"])));
}

fn consume_as_tuples(iter: &mut impl Iterator<Item=(ObjectPath, ValueWithMVCC)>, top_level: &ObjectPath, extract_values: ExtractValue) -> Vec<Tuple> {
    let mut iter = iter.peekable();
    let mut tuples = Vec::new();
    loop {
        let peek = match iter.peek() {
            None => break,
            Some(x) => x
        };
        if peek.0.as_str().starts_with(top_level.as_str()) {
            let tuple = consume_single_tuple(&mut iter, top_level, &extract_values);
            if !tuple.1.is_empty() {
                tuples.push(tuple);
            }
        }
    };
    tuples
}

use serde_json::Number;

fn typed_value_to_json(t: TypedValue) -> Option<Value> {
    match t {
        TypedValue::String(s) => { Some(Value::String(s)) }
        TypedValue::Number(s) => { Some(Value::Number(Number::from_f64(s).unwrap())) }
        TypedValue::Deleted => { None }
    }
}

use crate::parsing::TableExpression;
use crate::parsing::TableExpression::NamedTable;

pub fn do_select_stmt(q: SelectQuery, db: &DbContext) {
    let from = match *q.from {
        TableExpression::NamedTable(s) => s,
        _ => panic!()
    }.into();

    let mut txn = ReplicatedTxn::new(&db);
    let ret = txn.read_range_owned(&from).unwrap();
    let mut iter = ret.into_iter();

    let ev = ExtractValue::from(q.column_list);
    dbg!(consume_as_tuples(&mut iter, &from, ev));
}

#[test]
fn test4() {
    let q = SelectQuery {
        distinct: false,
        column_list: vec![ColumnExpr::String("id".to_string()), ColumnExpr::String("tele".to_string())],
        where_exp: None,
        from: Box::new(NamedTable("/".to_string())),
    };

    let db = db!(
        "/user10/" = 42f64,
        "/user1/id/" = "fdsvcx",
        "/user1/fd3f/fdsavc42/" = 42f64,
        "/user1/tele/" = 4252424f64,

        "/user2/id/" = "fdsvcx",
        "/user2/tele/" = 4252424f64,

        "/user3/id/" = "fdsvcx",
        "/user3/tele/" = 4252424f64,

        "/user4/id/" = "fdsvcx",
        "/user4/tele/" = 4252424f64
    );

    do_select_stmt(q, &db);
}

#[test]
fn test3() {
    let db = db!("/test/and/" = 1f64, "/test/bat/" = 2f64, "/test/cat/" = 2f64,"/test/dog/" = 2f64,
        "/zther/a/" = "3", "/zther/afd/" = "3", "/zther/afx/" = "3", "/zther/fds/" = "3", "/zther/fdsav/" = "3",
        "/fdsvc/" = 5.32f64);

    let mut txn = ReplicatedTxn::new(&db);
    let ret = txn.read_range_owned(&"/".into()).unwrap();
    let mut iter = ret.into_iter();
    dbg!(consume_as_tuples(&mut iter, &"/".into(), ExtractValue::All));
}