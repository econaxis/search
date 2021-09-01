use serde::{Serialize, Deserialize};
use std::fmt::{Display, Formatter};

impl TypedValue {
    pub fn as_str(&self) -> &str {
        match self {
            TypedValue::String(s) => s,
            _ => panic!()
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypedValue {
    String(String),
    Number(f64),
    Deleted,
}

impl Display for TypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use TypedValue::*;
        match self {
            String(s) => f.write_str(s),
            Number(a) => a.fmt(f),
            Deleted => f.write_str("null")
        }
    }
}

impl PartialEq for TypedValue {
    fn eq(&self, other: &Self) -> bool {
        use TypedValue::*;
        match (self, other) {
            (String(s), String(o)) => s == o,
            (Number(n), Number(o)) => (n - o).abs().le(&0.00001f64),
            (Deleted, Deleted) => true,
            _ => panic!("Can't compare two values of different types")
        }
    }
}

impl From<f64> for TypedValue {
    fn from(a: f64) -> Self {
        Self::Number(a)
    }
}

impl From<std::string::String> for TypedValue {
    fn from(a: std::string::String) -> Self {
        Self::String(a)
    }
}

impl<'a> From<&'a str> for TypedValue {
    fn from(a: &'a str) -> Self {
        Self::from(a.to_string())
    }
}

impl From<TypedValue> for String {
    fn from(a: TypedValue) -> Self {
        match a {
            TypedValue::String(s) => s,
            _ => panic!()
        }
    }
}