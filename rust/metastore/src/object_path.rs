use std::borrow::Borrow;
use std::collections::Bound;
use std::fmt::{Display, Formatter};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ObjectPath(pub Vec<u8>);

impl From<String> for ObjectPath {
    fn from(a: String) -> Self {
        Self(a.into_bytes())
    }
}

impl Display for ObjectPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&*String::from_utf8(self.0.clone()).unwrap())
    }
}

impl ObjectPath {
    pub fn new(str: &str) -> Self {
        Self(Vec::from(str))
    }

    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(self.0.as_slice()).unwrap()
    }

    pub fn concat<T: Borrow<str>>(&self, other: T) -> Self {
        let mut newone = self.0.clone();
        newone.push(b'/');
        newone.extend(other.borrow().as_bytes());
        Self(newone)
    }

    pub fn split_parts(&self) -> std::str::Split<'_, char> {
        std::str::from_utf8(
            // Starting with a slash, generates empty string in the beginning
            // (undesireable)
            &self.0[1..],
        )
        .unwrap()
        .split('/')
    }

    pub fn get_prefix_ranges(&self) -> (Bound<Self>, Bound<Self>) {
        let mut min = self.0.clone();
        let mut max = min.clone();

        min.extend([b'/', 1]);
        max.extend([b'/', 126]);

        let min: ObjectPath = ObjectPath::from(unsafe { String::from_utf8_unchecked(min) });
        let max: ObjectPath = ObjectPath::from(unsafe { String::from_utf8_unchecked(max) });

        (Bound::Included(min), Bound::Included(max))
    }
}

impl Default for ObjectPath {
    fn default() -> Self {
        let ret = Self(Vec::from("/user"));

        return ret;
    }
}

impl From<&str> for ObjectPath {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}
