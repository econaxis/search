pub mod btreemap_kv_backend;
mod lock_data_manager;
mod mvcc_metadata;
mod typed_value;
pub mod value_with_mvcc;

pub use mvcc_metadata::MVCCMetadata;
pub use typed_value::TypedValue;
pub use value_with_mvcc::{UnlockedWritableMVCC, ValueWithMVCC};

use crate::object_path::ObjectPath;
use crate::DbContext;

pub use lock_data_manager::{IntentMap, LockDataRef};
pub use mvcc_metadata::{WriteIntent, WriteIntentStatus};

use crate::rwtransaction_wrapper::MutBTreeMap;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::sync::RwLockReadGuard;

use crate::custom_error_impl;

#[derive(Debug, PartialEq)]
pub enum ReadError {
    ValueNotFound,
    PendingIntentErr(LockDataRef),
    Other(String),
}
custom_error_impl!(ReadError);

pub(super) fn update(
    ctx: &DbContext,
    key: &ObjectPath,
    new_value: TypedValue,
    txn: LockDataRef,
) -> Result<(), String> {
    match get_latest_mvcc_value(&ctx.db, key) {
        (lock, Some(res)) => {
            let resl = match res.get_readable_fix_errors(ctx, txn) {
                Ok(a) => a,
                Err(ReadError::ValueNotFound) => {
                    // Some other thread jumped in and deleted/modified this value before we could lock it.
                    // Call this function again with exact same parameters so it chooses the other branch (insert value) instead.
                    std::mem::drop(lock);
                    return update(ctx, key, new_value, txn);
                }
                Err(err) => {
                    return Err(format!("{:?}", err));
                }
            };
            let mut resl = res.get_writable(txn, resl)?;

            // Actually update the value with the desired new_value.
            resl.inplace_update(ctx, txn, new_value).unwrap();

            std::mem::drop(resl);
        }
        (lock, None) => {
            std::mem::drop(lock);
            // We're inserting a new key here.
            ctx.db.insert(
                key.clone(),
                ValueWithMVCC::new(txn, new_value),
                txn.timestamp,
            )?;
        }
    };

    Ok(())
}

fn check_has_value(db: &MutBTreeMap, key: &ObjectPath) -> bool {
    db.get_mut(key).is_some()
}

fn get_latest_mvcc_value<'a>(
    db: &'a MutBTreeMap,
    key: &ObjectPath,
) -> (
    RwLockReadGuard<'a, UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>,
    Option<&'a mut ValueWithMVCC>,
) {
    // todo! right now, we locking the whole database for each single read/write (not a transaction, which is good).
    // this because there's no atomic way to set an enum (WriteIntent) right now.
    // Also because I haven't thought of a way to do low-level per-value locking
    // In MVCC and MVTO model, readers don't block writers.
    // It's also true in this DB, but low-level locking comes in when setting atomically timestamp values, or
    // changing the String atomically. In these cases, we must lock the value for a brief moment to do operations, then unlock it.
    // Without this, readers might read invalid memory and will segfault.

    db.get_mut_with_lock(key)
}

pub fn read_reference(
    ctx: &DbContext,
    v: &ValueWithMVCC,
    txn: LockDataRef,
) -> Result<ValueWithMVCC, ReadError> {
    enum R<'a> {
        Result(ValueWithMVCC),
        Recurse(&'a mut ValueWithMVCC),
    }

    fn do_read<'a>(
        res: &ValueWithMVCC,
        ctx: &'a DbContext,
        txn: LockDataRef,
    ) -> Result<R<'a>, ReadError> {

        let read_latest = res.get_readable_fix_errors(ctx, txn);

        match read_latest {
            Ok(resl) => {
                resl.confirm_read(txn.timestamp);
                let cloned = ValueWithMVCC::from_tuple(resl.meta.clone(), resl.val.clone());
                Ok(R::Result(cloned))
            }
            Err(err) => {
                let resl = res.get_mvcc_copy();
                if resl.get_beg_time() >= txn.timestamp {
                    return if let Ok(prevval) = resl.get_prev_mvcc(ctx) {
                        Ok(R::Recurse(prevval))
                    } else {
                        // We've reached beginning of version chain, and yet the timestamp is smaller than the begin timestamp.
                        Err(ReadError::ValueNotFound)
                    };
                } else {
                    Err(err)
                }
            }
        }
    }
    let mut res = do_read(v, ctx, txn)?;

    while let R::Recurse(recurse) = res {
        res = do_read(recurse, ctx, txn)?;
    }
    if let R::Result(r) = res {
        Ok(r)
    } else {
        unreachable!()
    }
}

pub fn read(
    ctx: &DbContext,
    key: &ObjectPath,
    txn: LockDataRef,
) -> Result<ValueWithMVCC, ReadError> {
    let (_lock, res) = get_latest_mvcc_value(&ctx.db, key);
    let res = res.ok_or_else(|| "Read value doesn't exist".to_string())?;

    read_reference(ctx, res, txn)
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::rwtransaction_wrapper::ReplicatedTxn;

    #[test]
    fn regression_wrong_error_emitted() {
        let db = db!();
        let mut txn1 = ReplicatedTxn::new(&db);
        let mut txn2 = ReplicatedTxn::new(&db);
        txn1.write(&"/test/a".into(), "a".into()).unwrap();
        assert_matches!(txn2.read_range_owned(&"/test/".into()), Err(..));
        txn1.commit();
        assert_matches!(txn2.read_range_owned(&"/test/".into()), Ok(..));
    }

    #[test]
    fn writes_dont_block_reads() {
        let db = db!("k" = "v", "k1" = "v1");
        let mut r = ReplicatedTxn::new(&db);
        let mut w = ReplicatedTxn::new(&db);

        w.write(&"k".into(), "v1".into());
        assert_eq!(r.read(&"k".into()).unwrap(), "v".into());

        w.commit();
        let mut r = ReplicatedTxn::new(&db);
        assert_eq!(r.read(&"k".into()).unwrap(), "v1".into());
    }
}
