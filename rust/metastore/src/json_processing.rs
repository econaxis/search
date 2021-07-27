use std::collections::{VecDeque};
use std::fmt::{Display, Formatter};

use serde_json::Value;
use std::iter::FromIterator;
use crate::object_path::ObjectPath;

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
            Self::Boolean(bool) => f.write_str(&bool.to_string())
        }
    }
}

enum PrimValueOrOther {
    PrimitiveValue(PrimitiveValue),
    Other(Value),
}


pub fn json_to_map(json: Value) -> Vec<(ObjectPath, PrimitiveValue)> {
    let mut res = Vec::new();

    let mut obj_queue: VecDeque<(Value, ObjectPath)> = VecDeque::from_iter([(json, Default::default())]);

    while !obj_queue.is_empty() {
        let (obj, mut prefix) = obj_queue.pop_back().unwrap();

        let primvalue = match obj {
            Value::Number(n) => PrimValueOrOther::PrimitiveValue(PrimitiveValue::Number(n.as_f64().unwrap())),
            Value::String(str) => PrimValueOrOther::PrimitiveValue(PrimitiveValue::String(str)),
            Value::Bool(boolean) => PrimValueOrOther::PrimitiveValue(PrimitiveValue::Boolean(boolean)),
            Value::Null => PrimValueOrOther::PrimitiveValue(PrimitiveValue::String("null".to_string())),
            _ => PrimValueOrOther::Other(obj)
        };

        match primvalue {
            PrimValueOrOther::PrimitiveValue(val) => {
                prefix.make_correct_suffix();
                res.push((prefix, val));
            }

            PrimValueOrOther::Other(val) => {
                let additions: VecDeque<(Value, ObjectPath)> = match val {
                    Value::Array(vec) => {
                        let length = (Value::Number(serde_json::Number::from(vec.len())), prefix.concat("length"));
                        let mut elems: VecDeque<_> = vec.into_iter()
                            .enumerate()
                            .map(|(index, elem)| (elem, prefix.concat(index.to_string()))).collect();

                        elems.push_back(length);
                        elems
                    }
                    Value::Object(obj) => {
                        obj.into_iter().map(|(key, value)| (value, prefix.concat(key))).collect()
                    }
                    _ => unreachable!()
                };

                obj_queue.extend(additions.into_iter());
            }
        };
    };

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
        Value::Object(map) => if map.is_empty() {
            false
        } else {
            map.iter().all(|elem| check_valid_json(elem.1))
        },
        _ => true
    }
}

pub fn create_materialized_path<RawValue: ToString>(json: &mut Value, path: &[&str], final_value: RawValue) {
    match json {
        Value::Object(..) => (),
        Value::Null => {
            *json = Value::Object(serde_json::Map::new())
        }
        _ => {
            println!("{:?}, {:?}", json, path);
            panic!("JSON value already exists and is not object, ")
        }
    };

    let obj = json.as_object_mut().unwrap();
    if path.len() == 1 {
        obj.insert(path[0].to_owned(), Value::String(final_value.to_string()));
    } else if path.len() > 1 {
        if !obj.contains_key(path[0]) {
            obj.insert(path[0].to_owned(), Value::Null);
        }
        create_materialized_path(obj.get_mut(path[0]).unwrap(), &path[1..], final_value);
    } else {
        unreachable!()
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    

    #[test]
    fn materialized_test() {
        let mut emptyjson = Value::Object(serde_json::Map::new());

        create_materialized_path(&mut emptyjson, &["user", "obj", "obj1", "obj2", "float"], PrimitiveValue::Number(5.0));

        assert_eq!(emptyjson, serde_json::json!({
            "user": {"obj": {"obj1": {"obj2": {"float": "5"}}}}
        }));

        create_materialized_path(&mut emptyjson, &["user", "obj", "obj1", "obj2", "float2"], PrimitiveValue::Number(5.0));
        assert_eq!(emptyjson, serde_json::json!({
            "user": {"obj": {"obj1": {"obj2": {"float": "5", "float2": "5"}}}}
        }));
        create_materialized_path(&mut emptyjson, &["user", "obj", "float3"], PrimitiveValue::Number(5.0));
        assert_eq!(emptyjson, serde_json::json!({
            "user": {"obj": {"obj1": {"obj2": {"float": "5", "float2": "5"}}, "float3": "5"}}
        }));
    }


}


