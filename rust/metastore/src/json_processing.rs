use std::collections::VecDeque;
use std::fmt::{Display, Formatter};

use crate::object_path::ObjectPath;
use serde_json::Value;
use std::iter::FromIterator;

#[derive(Debug)]
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
    let mut res = Vec::new();

    let mut obj_queue: VecDeque<(Value, ObjectPath)> =
        VecDeque::from_iter([(json, Default::default())]);

    while !obj_queue.is_empty() {
        let (obj, prefix) = obj_queue.pop_back().unwrap();

        let primvalue = match obj {
            Value::Number(n) => {
                PrimValueOrOther::PrimitiveValue(PrimitiveValue::Number(n.as_f64().unwrap()))
            }
            Value::String(str) => PrimValueOrOther::PrimitiveValue(PrimitiveValue::String(str)),
            Value::Bool(boolean) => {
                PrimValueOrOther::PrimitiveValue(PrimitiveValue::Boolean(boolean))
            }
            Value::Null => {
                PrimValueOrOther::PrimitiveValue(PrimitiveValue::String("null".to_string()))
            }
            _ => PrimValueOrOther::Other(obj),
        };

        match primvalue {
            PrimValueOrOther::PrimitiveValue(val) => {
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
                    Value::Object(obj) => obj
                        .into_iter()
                        .map(|(key, value)| (value, prefix.concat(key)))
                        .collect(),
                    _ => unreachable!(),
                };

                obj_queue.extend(additions.into_iter());
            }
        };
    }
    res
}

pub fn create_materialized_path<RawValue: ToString>(
    json: &mut Value,
    path: &[&str],
    final_value: RawValue,
) {
    match json {
        Value::Object(..) => (),
        Value::Null => *json = Value::Object(serde_json::Map::new()),
        _ => {
            panic!("JSON value already exists and is not object")
        }
    };

    let obj = json.as_object_mut().unwrap();
    if path.len() == 1 {
        obj.insert(path[0].to_owned(), Value::String(final_value.to_string()));
    } else if path.len() > 1 {
        if !obj.contains_key(path[0]) {
            obj.insert(path[0].to_owned(), Value::Object(serde_json::Map::new()));
        }
        create_materialized_path(obj.get_mut(path[0]).unwrap(), &path[1..], final_value);
    } else {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

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

    fn test_json() -> Vec<(ObjectPath, PrimitiveValue)> {
        let _map = BTreeMap::<ObjectPath, i64>::new();

        let json = serde_json::json!({"nested": {
            "nested_arr": [1, 4, 2, 5, 6, 3, 2]
        }, "nested_2": {
            "test": true,
            "ads": Value::Null,
            "vcxf": [Value::Null, 5, "fdvc"]
        }});

        json_to_map(json)
    }
}
