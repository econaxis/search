use std::collections::VecDeque;
use std::fmt::{Display, Formatter};

use metastore::object_path::ObjectPath;
use serde_json::Value;
use std::iter::FromIterator;

#[derive(Debug, Clone)]
pub enum PrimitiveValue {
    String(String),
    Number(f64),
    Boolean(bool),
}

impl Display for PrimitiveValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(str) => f.write_str(&str.to_string()),
            Self::Number(num) => f.write_str(&num.to_string()),
            Self::Boolean(bool) => f.write_str(&bool.to_string()),
        }
    }
}

enum PrimValueOrOther {
    PrimitiveValue(PrimitiveValue),
    Other(Value),
}

pub fn json_to_map(json: Value) -> Vec<(ObjectPath, PrimitiveValue)> {
    type V = Value;
    type PV = PrimitiveValue;

    let mut res = Vec::new();

    let mut obj_queue: VecDeque<(Value, ObjectPath)> =
        VecDeque::from_iter([(json, ObjectPath::from(""))]);

    while !obj_queue.is_empty() {
        let (obj, mut prefix) = obj_queue.pop_back().unwrap();

        let primvalue = match obj {
            V::Number(n) => PrimValueOrOther::PrimitiveValue(PV::Number(n.as_f64().unwrap())),
            V::String(str) => PrimValueOrOther::PrimitiveValue(PV::String(str)),
            V::Bool(boolean) => PrimValueOrOther::PrimitiveValue(PV::Boolean(boolean)),
            V::Null => PrimValueOrOther::PrimitiveValue(PV::String("null".to_string())),
            _ => PrimValueOrOther::Other(obj),
        };

        match primvalue {
            PrimValueOrOther::PrimitiveValue(val) => {
                prefix.make_correct_suffix();
                res.push((prefix, val));
            }

            PrimValueOrOther::Other(val) => {
                let additions: VecDeque<(Value, ObjectPath)> = match val {
                    Value::Array(vec) => {
                        let length = (
                            Value::Number(serde_json::Number::from(vec.len())),
                            prefix.concat("length"),
                        );
                        let mut elems: VecDeque<_> = vec
                            .into_iter()
                            .enumerate()
                            .map(|(index, elem)| (elem, prefix.concat(index.to_string())))
                            .collect();

                        elems.push_back(length);
                        elems
                    }
                    Value::Object(obj) => {
                        let type_specifier = (
                            Value::String("object-type-placeholder".to_string()),
                            prefix.clone(),
                        );
                        let mut result: VecDeque<_> = obj
                            .into_iter()
                            .map(|(key, value)| (value, prefix.concat(key)))
                            .collect();
                        result.push_back(type_specifier);
                        result
                    }
                    _ => unreachable!(),
                };

                obj_queue.extend(additions.into_iter());
            }
        };
    }

    res
}

pub fn map_to_json(map: &Vec<(ObjectPath, PrimitiveValue)>) -> Value {
    let mut start = Value::Null;

    map.iter().for_each(|(path, value)| {
        let split: Vec<_> = path.split_parts().collect();
        create_materialized_path(&mut start, &split, value);
    });

    start
}

pub fn check_valid_json(json: &Value) -> bool {
    match json {
        Value::Object(map) => {
            if map.is_empty() {
                false
            } else {
                map.iter().all(|elem| check_valid_json(elem.1))
            }
        }
        _ => true,
    }
}

const MAXJSONDEPTH: usize = 1000;

pub fn create_materialized_path<RawValue: ToString>(
    json: &mut Value, // Input-parameter
    path: &[&str],
    final_value: RawValue,
) {
    match json {
        Value::Object(..) => (),
        Value::Null => *json = Value::Object(serde_json::Map::new()),
        _ => {
            if json.as_str() != Some("object-type-placeholder") {
                println!("{:?}, {:?}", json, path);
                panic!("JSON value already exists and is not object, ")
            }
            *json = Value::Object(serde_json::Map::new());
        }
    };

    let obj = json.as_object_mut().unwrap();
    match path.len() {
        1 => {
            obj.insert(path[0].to_owned(), Value::String(final_value.to_string()));
        }
        1..=MAXJSONDEPTH => {
            if !obj.contains_key(path[0]) {
                obj.insert(path[0].to_owned(), Value::Null);
            }
            create_materialized_path(obj.get_mut(path[0]).unwrap(), &path[1..], final_value);
        }
        0 => unreachable!(),
        _ => panic!("Maximum JSON depth exceeded"),
    };
}

#[cfg(test)]
mod tests {
    use crate::json_request_writers::json_processing::{
        create_materialized_path, json_to_map, PrimitiveValue,
    };
    use metastore::db_context::create_replicated_context;
    use metastore::{ObjectPath, ReplicatedTxn, TypedValue};
    use serde_json::Value;
    use std::borrow::Cow;
    use std::borrow::Cow::Borrowed;

    #[test]
    fn materialized_test() {
        let mut emptyjson = Value::Object(serde_json::Map::new());

        create_materialized_path(
            &mut emptyjson,
            &["user", "obj", "obj1", "obj2", "float"],
            PrimitiveValue::Number(5.0),
        );

        assert_eq!(
            emptyjson,
            serde_json::json!({
                "user": {"obj": {"obj1": {"obj2": {"float": "5"}}}}
            })
        );

        create_materialized_path(
            &mut emptyjson,
            &["user", "obj", "obj1", "obj2", "float2"],
            PrimitiveValue::Number(5.0),
        );
        assert_eq!(
            emptyjson,
            serde_json::json!({
                "user": {"obj": {"obj1": {"obj2": {"float": "5", "float2": "5"}}}}
            })
        );
        create_materialized_path(
            &mut emptyjson,
            &["user", "obj", "float3"],
            PrimitiveValue::Number(5.0),
        );
        assert_eq!(
            emptyjson,
            serde_json::json!({
                "user": {"obj": {"obj1": {"obj2": {"float": "5", "float2": "5"}}, "float3": "5"}}
            })
        );
    }

    #[test]
    fn testbig() {
        let ctx = create_replicated_context();
        let ctx = &ctx;
        let mut txn0 = ReplicatedTxn::new(ctx);
        let mut txn1 = ReplicatedTxn::new(ctx);

        test_json()
            .into_iter()
            .enumerate()
            .for_each(|(index, (path, value))| {
                txn0.write(&path, TypedValue::from(value.to_string()))
                    .unwrap();
            });

        txn0.commit();
        txn1.commit();

        let mut txn2 = ReplicatedTxn::new(ctx);
        test_json().into_iter().for_each(|(path, value)| {
            assert_eq!(txn2.read(&path).unwrap().as_str(), value.to_string());
        });
        txn2.commit();
    }

    fn test_json() -> Vec<(ObjectPath, PrimitiveValue)> {
        let json = serde_json::json!({"nested": {
            "nested_arr": [1, 4, 2, {
                    "what": true,
                    "what2": "fdsav"
                }, 6, 3, 2]
        }, "nested_2": {
            "test": true,
            "ads": Value::Null,
            "vcxf": [Value::Null, 5, "fdvc"]
        }});

        json_to_map(json)
    }
}
