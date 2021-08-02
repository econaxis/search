use std::borrow::Cow::Borrowed;
use std::borrow::{Borrow, Cow};
use std::collections::Bound;
use std::fmt::{Display, Formatter};
use std::iter::FromIterator;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ObjectPath(String);

impl From<String> for ObjectPath {
    fn from(a: String) -> Self {
        Self(a)
    }
}
impl From<&str> for ObjectPath {
    fn from(a: &str) -> Self {
        Self::new(a)
    }
}

impl<BS: Borrow<str>> FromIterator<BS> for ObjectPath {
    fn from_iter<T: IntoIterator<Item = BS>>(iter: T) -> Self {
        iter.into_iter().fold(
            ObjectPath::new(""),
            |one: ObjectPath, two: BS| -> ObjectPath { one.concat(two) },
        )
    }
}

#[test]
fn test_from_iterator() {
    let v = vec!["user".to_string(), "one".to_string(), "two".to_string()];
    assert_eq!(
        ObjectPath::from_iter(v.into_iter()),
        ObjectPath::new("/user/one/two")
    )
}

impl Display for ObjectPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&*String::from_utf8(self.0.as_bytes().to_vec()).unwrap())
    }
}

impl ObjectPath {
    pub fn new(str: &str) -> Self {
        Self(String::from(str))
    }

    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(self.0.as_bytes()).unwrap()
    }

    pub fn as_cow_str(&self) -> Cow<str> {
        Borrowed(self.as_str())
    }

    pub fn concat<T: Borrow<str>>(&self, other: T) -> Self {
        let mut newone = self.0.clone();
        newone.push('/');
        newone.push_str(other.borrow());
        Self(newone)
    }

    pub fn split_parts(&self) -> std::str::Split<'_, char> {
        let len = self.0.len();
        std::str::from_utf8(
            // Starting with a slash, generates empty string in the beginning
            // (undesireable)
            &self.0[1..len - 1].as_ref(),
        )
        .unwrap()
        .split('/')
    }

    pub fn make_correct_suffix(&mut self) {
        if self.0.chars().last().unwrap() != '/' {
            self.0.push('/');
        }
    }

    pub fn get_prefix_ranges(&self) -> (Bound<Self>, Bound<Self>) {
        assert_eq!(self.0.chars().last().unwrap(), '/');

        let mut min = self.0.clone();
        let mut max = min.clone();

        let minlen = min.len();
        let maxlen = max.len();
        *unsafe { min.as_bytes_mut() }.get_mut(minlen - 1).unwrap() = 1u8;
        *unsafe { max.as_bytes_mut() }.get_mut(maxlen - 1).unwrap() = 126u8;

        let min: ObjectPath = ObjectPath::from(min);
        let max: ObjectPath = ObjectPath::from(max);

        (Bound::Included(min), Bound::Included(max))
    }
}

impl Default for ObjectPath {
    fn default() -> Self {
        let ret = Self::new("/user");

        return ret;
    }
}
