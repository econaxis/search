use crate::DbContext;
use crate::rwtransaction_wrapper::mvcc_manager::{LockDataRef, WriteIntent, WriteIntentStatus};
use crate::rwtransaction_wrapper::mvcc_manager::mvcc_metadata::WriteIntentError;
use crate::rwtransaction_wrapper::MVCCMetadata;
use crate::timestamp::Timestamp;
use std::sync::atomic::{AtomicBool, Ordering};


#[derive(Debug, Clone)]
pub struct ValueWithMVCC(MVCCMetadata, String);

pub struct UnlockedMVCC<'a>(pub &'a mut MVCCMetadata, pub &'a mut String);

impl<'a> UnlockedMVCC<'a> {
    pub(crate) fn clone_to_value(&self) -> ValueWithMVCC {
        let meta = self.0.clone();
        let str = self.1.clone();
        ValueWithMVCC(meta, str)
    }
}

impl<'a> UnlockedMVCC<'a> {
    fn swap(&mut self, meta: MVCCMetadata, val: String) -> ValueWithMVCC {
        let old = std::mem::replace(self.0, meta);
        let oldstr = std::mem::replace(self.1, val);

        ValueWithMVCC(old, oldstr)
    }
    // TODO: restructure
    pub(in super) fn become_newer_version(
        &mut self,
        ctx: &DbContext,
        txn: LockDataRef,
        newvalue: String,
    ) -> Result<(), String> {


        // If we're rewriting our former value, then that value never got committed, so it hsouldn't be archived
        let txn_rewrite = match self.0.get_write_intents(&ctx.transaction_map) {
            Some((WriteIntentStatus::Pending, wi)) => wi.associated_transaction == txn,
            _ => false
        };

        assert_eq!(self.0.get_write_intents(&ctx.transaction_map).unwrap().1.associated_transaction, txn);

        let newmvcc = self.0.deactivate_and_get_successor(txn.timestamp);

        assert_eq!(newmvcc.get_write_intents(&ctx.transaction_map).unwrap().1.associated_transaction, txn);
        assert_eq!(self.0.get_write_intents(&ctx.transaction_map).unwrap().1.associated_transaction, txn);

        let archived = self.swap(newmvcc, newvalue);

        if !txn_rewrite {
            let oldmetadata_key = ctx
                .old_values_store
                .insert(archived);

            self.0.previous_mvcc_value.insert(oldmetadata_key);
        }
        Ok(())
    }
    fn rescue_previous_value(&mut self, ctx: &DbContext) {
        // TODO: dangerous multithreading, must use atomics/lock the whole ValueWithMVCC up preferably
        let cur_end_ts = self.0.end_ts;

        if let Some(prev) = self.0.previous_mvcc_value {
            let (mut oldmeta, oldstr) = ctx.old_values_store.remove(prev).into_inner();
            oldmeta.end_ts = cur_end_ts;
            std::mem::replace(self.0, oldmeta);
            std::mem::replace(self.1, oldstr);

            // There must be no write intent on the previous value (or else, how could it have gotten overwritten?)
            assert!(self.0.get_write_intents(&ctx.transaction_map).is_none());
        } else {
            // *self.1 = "0".to_string();
        }
    }
}

static SPIN: AtomicBool = AtomicBool::new(false);

struct Guard();

impl Guard {
    fn new() -> Self {
        while !SPIN.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {};
        Self()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        SPIN.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).unwrap();
    }
}

impl ValueWithMVCC {
    pub fn new(txn: LockDataRef, value: String) -> Self {
        let meta = MVCCMetadata::new(txn);

        Self(meta, value)
    }

    pub fn into_inner(self) -> (MVCCMetadata, String) {
        (self.0, self.1)
    }
    pub fn as_inner(&self) -> (&MVCCMetadata, &String) {
        (&self.0, &self.1)
    }

    pub fn from_tuple(a: MVCCMetadata, v: String) -> Self {
        Self(a, v)
    }

    pub fn lock_for_write(&mut self, ctx: &DbContext, txn: LockDataRef) -> Result<UnlockedMVCC<'_>, String> {
        // If we don't guard, then we might clear write intents twice, e.g. ...
        let _guard = Guard::new();

        let res = self.check_write_intents(ctx, txn);

        let is_aborted = if let Err(err) = res {
            match err {
                WriteIntentError::Aborted(x) => { self.0.clear_write_intents(x) }
                WriteIntentError::WritingInThePast => Err("Write intent error"),
                WriteIntentError::PendingIntent(_) => Err("Write intent error"),
                WriteIntentError::Other(_) => Err("Write intent error")
            }?;
            true
        } else {
            false
        };


        // We actually have to check for ourselves again, because we need the writeintent value to do a proper compare and swap.
        self.0.atomic_insert_write_intents(WriteIntent { associated_transaction: txn })?;

        self.check_read(ctx, txn)?;
        let mut unlocked = UnlockedMVCC(&mut self.0, &mut self.1);
        if is_aborted {
            unlocked.rescue_previous_value(ctx);
        }

        // while !SPIN.compare_exchange(true,false, Ordering::SeqCst,Ordering::SeqCst).is_ok() {}

        Ok(unlocked)
    }


    fn check_write_intents(&mut self, ctx: &DbContext, txn: LockDataRef) -> Result<(), WriteIntentError> {
        self.0.check_write_intents(&ctx.transaction_map, txn)
    }


    pub fn check_read(&mut self, txnmap: &DbContext, txn: LockDataRef) -> Result<(), String> {
        match self.check_write_intents(txnmap, txn) {
            Ok(_) => Ok(()),
            // Fix the error if it's aborted
            Err(WriteIntentError::Aborted(_)) => {
                self.lock_for_write(txnmap, txn).map(|_| ())
            }
            Err(err) => Err(err.tostring())
        }?;


        self.0.check_read(&txnmap.transaction_map, txn)?;
        self.0.confirm_read(txn);
        Ok(())
    }
}
