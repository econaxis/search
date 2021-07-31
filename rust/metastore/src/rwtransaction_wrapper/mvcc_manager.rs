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
use std::time::Duration;
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
        let (lock,res) = get_latest_mvcc_value(&ctx.db, key);
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
    let res = db.get_mut_with_lock(key);
    res
}

fn get_correct_mvcc_value<'a>(
    ctx: &'a DbContext,
    key: &ObjectPath,
    txn: LockDataRef,
) -> Result<ValueWithMVCC, String> {
    let (lock,mut res) = get_latest_mvcc_value(&ctx.db, key);
    let res = res.unwrap();
    let read_latest = res.check_read(&ctx, txn);



    let mut error_causes = String::new();

    if read_latest.is_ok() {
        let val = res.as_inner().1.parse::<u64>().unwrap();
        let mut res = res.clone();
        std::mem::drop(lock);
        if val >= 100000 {
            unreachable!()
        };
        return Ok(res);
    } else {
        error_causes.push_str(&*read_latest.unwrap_err());
    }

    return Err("unabl tor ead".to_string());

    let mut prevoption = res.as_inner().0.previous_mvcc_value;
    while let Some(prev) = prevoption {
        let mut prevval = ctx.old_values_store.get_mut(prev).clone();

        let checkresult = prevval.check_read(&ctx, txn);

        if checkresult.is_ok() {
            let val = prevval.as_inner().1.parse::<u64>().unwrap();
            if val >= 100000 {
                let result = prevval.check_read(&ctx, txn);

                let key = prevval.as_inner().1.clone();
                unreachable!()
            };
            return Ok(prevval);
        } else {
            error_causes.push_str(&*checkresult.unwrap_err());
            prevoption = prevval.into_inner().0.previous_mvcc_value;
        }
    }
    Err(error_causes)
}

pub(super) fn read(
    ctx: &DbContext,
    key: &ObjectPath,
    cur_txn: LockDataRef,
) -> Result<ValueWithMVCC, String> {
    let mut v = get_correct_mvcc_value(ctx, key, cur_txn)?;
    v.check_read(&ctx, cur_txn)?;
    Ok(v)
}

