mod mvcc_metadata;
mod lock_data_manager;
mod kv_backend;
mod btreemap_kv_backend;
pub mod value_with_mvcc;

pub use mvcc_metadata::MVCCMetadata;
pub use value_with_mvcc::{ValueWithMVCC, UnlockedMVCC};

use crate::DbContext;
use crate::object_path::ObjectPath;

pub use btreemap_kv_backend::MutBTreeMap;

pub use lock_data_manager::{LockDataRef, IntentMap};
pub use mvcc_metadata::{WriteIntent, WriteIntentStatus};

use std::collections::BTreeMap;
use std::cell::UnsafeCell;
use std::sync::MutexGuard;

pub(super) fn update<'a>(
    ctx: &'a DbContext,
    key: &ObjectPath,
    new_value: String,
    txn: LockDataRef,
) -> Result<&'a ValueWithMVCC, String> {
    let ret = if check_has_value(&ctx.db, key) {
        let (lock, res) = get_latest_mvcc_value(&ctx.db, key);
        let res = res.unwrap();
        let mut resl = res.lock_for_write(ctx, txn)?;
        assert_eq!(resl.0.get_write_intents(&ctx.transaction_map).unwrap().1.associated_transaction, txn);
        std::mem::drop(lock);
        // todo! delaying dropping lock somehow fixes everything...
        resl.become_newer_version(ctx, txn, new_value);

        res
    } else {
        // We're inserting a new key here.
        ctx.db
            .insert(key.clone(), ValueWithMVCC::new(txn, new_value));

        get_latest_mvcc_value(&ctx.db, key).1.unwrap()
    };

    Ok(ret)
}

fn check_has_value(db: &MutBTreeMap, key: &ObjectPath) -> bool {
    db.get_mut(key).is_some()
}

fn get_latest_mvcc_value<'a>(db: &'a MutBTreeMap, key: &ObjectPath) -> (MutexGuard<'a, UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>, Option<&'a mut ValueWithMVCC>) {
    // todo! right now, we locking the whole database for each single read/write (not a transaction, which is good).
    // this because there's no atomic way to set an enum (WriteIntent) right now.
    // Also because I haven't thought of a way to do low-level per-value locking
    // In MVCC and MVTO model, readers don't block writers.
    // It's also true in this DB, but low-level locking comes in when setting atomically timestamp values, or
    // changing the String atomically. In these cases, we must lock the value for a brief moment to do operations, then unlock it.
    // Without this, readers might read invalid memory and will segfault.
    let res = db.get_mut_with_lock(key);
    res
}

fn get_correct_mvcc_value(
    ctx: &DbContext,
    key: &ObjectPath,
    txn: LockDataRef,
) -> Result<ValueWithMVCC, String> {
    let (lock, res) = get_latest_mvcc_value(&ctx.db, key);
    let res = res.unwrap();
    let mut read_latest = res.check_read(&ctx, txn);

    let fixed = if let Err(ref err) = read_latest {
        res.fix_errors(&ctx, txn, err.clone())
    } else {
        Ok(())
    };

    let mut error_causes = String::new();

    if fixed.is_ok() {
        let _val = res.as_inner().1.parse::<u64>().unwrap();
        let res = res.clone();
        return Ok(res);
    } else {
        error_causes.push_str(&*read_latest.unwrap_err().tostring());
    }
    std::mem::drop(lock);


    let mut prevoption = res.as_inner().0.get_prev_mvcc();
    while let Some(prev) = prevoption {
        let mut prevval = ctx.old_values_store.get_mut(prev).clone();
        let checkresult = prevval.check_read(&ctx, txn);

        if checkresult.is_ok() {
            return Ok(prevval);
        } else {
            error_causes.push_str(&*checkresult.unwrap_err().tostring());
            prevoption = prevval.into_inner().0.get_prev_mvcc();
        }
    }
    Err(error_causes)
}

pub(super) fn read(
    ctx: &DbContext,
    key: &ObjectPath,
    cur_txn: LockDataRef,
) -> Result<ValueWithMVCC, String> {
    let v = get_correct_mvcc_value(ctx, key, cur_txn)?;
    Ok(v)
}

