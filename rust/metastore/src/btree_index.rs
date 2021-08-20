use crate::{ObjectPath};
use crate::replicated_slave::SelfContainedDb;
use crate::rwtransaction_wrapper::{LockDataRef, TOMBSTONE};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use parking_lot::Mutex;
use crate::retry5;

struct Matcher {
    root: ObjectPath,
    item: String,
}

struct BtreeIndexInternal {
    db: SelfContainedDb,
    is_unique: bool,
    contained_keys: Mutex<HashSet<u64>>,
}

fn hash_object_path(key: &ObjectPath) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(key.as_str().as_bytes());
    hasher.finish()
}


fn retry<T, E, F: Fn() -> Result<T, E>>(f: F) -> Result<T, E> {
    let mut iters = 0;
    loop {
        iters += 1;
        let res = f();
        if res.is_ok() {
            return res;
        }
        if iters > 5 {
            return res;
        }
    }
}

impl BtreeIndexInternal {
    pub fn reupdate_key(&self, txn: LockDataRef, key: &ObjectPath, prevvalue: &str, newvalue: String) -> Result<(), String> {
        assert!(self.contained_keys.lock().get(&hash_object_path(key)).is_some());

        let value_as_obj = ObjectPath::from(prevvalue);
        let newvalue = ObjectPath::from(newvalue);

        retry(
            || match self.db.serve_read(txn, &value_as_obj) {
                Ok(val) => {
                    assert_eq!(val.get_val(), key.as_str());
                    // Delete this node
                    self.db.serve_write(txn, &value_as_obj, TOMBSTONE.into())?;

                    // Insert the new node
                    self.db.serve_write(txn, &newvalue, key.as_str().into())?;
                    Ok(())
                }
                Err(a) => return Err(a)
            }
        );

        Ok(())
    }

    pub fn insert_key(&self, txn: LockDataRef, key: &ObjectPath, value: String) {
        let not_contained_already = self.contained_keys.lock().insert(hash_object_path(key));
        assert!(not_contained_already);

        let value_as_obj = ObjectPath::from(value);

        retry::<_, String, _>(
            || match self.db.serve_read(txn, &value_as_obj) {
                // todo: string comparisons bad, should've used enum errors
                Err(err) if err == r#"Other("Read value doesn't exist")"# => {
                    self.db.serve_write(txn, &value_as_obj, key.as_str().into())?;
                    Ok(())
                }
                // Currently only unique indexes are supported
                _ => panic!("Trying to insert duplicate key into index")
            }
        );
    }
    pub fn query_key(&self, txn: LockDataRef, value: String) -> Result<ObjectPath, String> {
        let objpath = ObjectPath::from(value);
        let res = retry::<String, String, _>(
            || Ok(self.db.serve_read(txn, &objpath)?.into_inner().1)
        );
        res.map(ObjectPath::from)
    }

    pub fn commit(&self, txn: LockDataRef) {
        self.db.commit(txn);
    }

    pub fn abort(&self, txn: LockDataRef) {
        self.db.abort(txn);
    }
    pub fn new() -> Self { Self { db: SelfContainedDb::default(), is_unique: true, contained_keys: Default::default() } }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index() {
        let idx = BtreeIndexInternal::new();
        let txn = LockDataRef::debug_new(2);
        idx.insert_key(txn, &"/test/value".into(), "true".into());
        idx.commit(txn);


        let txn = LockDataRef::debug_new(3);
        dbg!(idx.query_key(txn, "true".into()));
    }

    #[test]
    #[should_panic]
    fn test_index_duplicate() {
        let idx = BtreeIndexInternal::new();
        let txn = LockDataRef::debug_new(2);
        idx.insert_key(txn, &"/test/value".into(), "true".into());
        idx.insert_key(txn, &"/test/value2".into(), "true".into());
        idx.commit(txn);
    }

    #[test]
    fn reupdate_index() {
        let idx = BtreeIndexInternal::new();
        let txn = LockDataRef::debug_new(2);
        let txn2 = LockDataRef::debug_new(4);

        idx.insert_key(txn, &"/test/value".into(), "true".into());
        dbg!(idx.query_key(txn, "true".into()));
        idx.reupdate_key(txn, &"/test/value".into(), "true", "false".into()).unwrap();
        dbg!(idx.query_key(txn, "false".into()));
        idx.insert_key(txn, &"/test/value2".into(), "true1".into());
        dbg!(idx.query_key(txn, "true1".into()));

        idx.reupdate_key(txn, &"/test/value2".into(), "true1", "true2".into()).unwrap();
        dbg!(idx.query_key(txn, "true2".into()));
        assert_matches!(idx.query_key(txn2, "true2".into()), Err(..));
        idx.commit(txn);
        assert_eq!(idx.query_key(txn2, "true2".into()), Ok(ObjectPath::from("/test/value2")));
    }
}