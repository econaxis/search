use crate::DbContext;
use crate::rwtransaction_wrapper::mvcc_manager::{LockDataRef, WriteIntent, WriteIntentStatus};
use crate::rwtransaction_wrapper::mvcc_manager::mvcc_metadata::WriteIntentError;
use crate::rwtransaction_wrapper::MVCCMetadata;

use std::sync::atomic::{AtomicBool, Ordering};


#[derive(Debug, PartialEq, Clone)]
pub struct ValueWithMVCC(MVCCMetadata, String);

pub struct UnlockedMVCC<'a>(pub &'a mut MVCCMetadata, pub &'a mut String);


// todo! every modification must lock, because there might be concurrent readers.
impl<'a> UnlockedMVCC<'a> {
    pub fn release_lock(mut self, txn: LockDataRef) {
        self.0.aborted_reset_write_intent(txn, None);
    }

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

        let oldmvcc = self.0.into_newer(txn.timestamp);
        let oldvalue = std::mem::replace(self.1, newvalue);

        assert_eq!(oldmvcc.get_write_intents(&ctx.transaction_map).unwrap().1.associated_transaction, txn);
        assert_eq!(self.0.get_write_intents(&ctx.transaction_map).unwrap().1.associated_transaction, txn);

        let archived = ValueWithMVCC(oldmvcc, oldvalue);

        if !txn_rewrite {
            let oldmetadata_key = ctx
                .old_values_store
                .insert(archived);

            self.0.insert_prev_mvcc(oldmetadata_key);
        }
        Ok(())
    }
    fn rescue_previous_value(&mut self, ctx: &DbContext) {
        // TODO: dangerous multithreading, must use atomics/lock the whole ValueWithMVCC up preferably
        let cur_end_ts = self.0.get_end_time();

        if let Some(prev) = self.0.get_prev_mvcc() {
            let (mut oldmeta, oldstr) = ctx.old_values_store.remove(prev).into_inner();
            oldmeta.set_end_time(cur_end_ts);
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
}

impl ValueWithMVCC {
    // pub fn try_clone(&self, txn: LockDataRef) -> Result<Self, String> {
    //     let mvccclone = self.0.try_clone(txn)?;
    //     let valueclone = self.1.clone();
    //     Ok(Self(mvccclone, valueclone))
    // }

    pub fn lock_for_write(&mut self, ctx: &DbContext, txn: LockDataRef) -> Result<UnlockedMVCC<'_>, String> {
        let res = self.check_read(ctx, txn);

        let is_aborted = if let Err(err) = res {
            match err {
                WriteIntentError::Aborted(x) => { self.0.aborted_reset_write_intent(x, Some(txn)) }
                WriteIntentError::WritingInThePast => Err("Past intent error".to_string()),
                WriteIntentError::PendingIntent(_) => Err("Pending intent error".to_string()),
                WriteIntentError::Other(x) => {
                    Err(format!("Other intent error {}", x))
                }
            }?;
            true
        } else {
            false
        };
        // We actually have to check for ourselves again, because we need the writeintent value to do a proper compare and swap.
        self.0.atomic_insert_write_intents(WriteIntent { associated_transaction: txn })?;

        self.check_read(ctx, txn).map_err(WriteIntentError::tostring)?;
        self.0.check_write(&ctx.transaction_map, txn)?;
        let mut unlocked = UnlockedMVCC(&mut self.0, &mut self.1);
        if is_aborted {
            unlocked.rescue_previous_value(ctx);
        }

        Ok(unlocked)
    }



    fn check_write_intents(&mut self, ctx: &DbContext, txn: LockDataRef) -> Result<(), WriteIntentError> {
        self.0.check_write_intents(&ctx.transaction_map, txn)
    }

    pub fn fix_errors(&mut self, txnmap: &DbContext, txn: LockDataRef, err: WriteIntentError) -> Result<(), String> {

        match err {
            WriteIntentError::Aborted(_) => {
                let unlocked = self.lock_for_write(txnmap, txn)?;
                unlocked.release_lock(txn);
                Ok(())
            }
            _ => Err(err.tostring())
        }?;

        self.check_write_intents(txnmap, txn).map_err(WriteIntentError::tostring)?;

        self.0.check_read(&txnmap.transaction_map, txn).map_err(|a| WriteIntentError::Other(a)).map_err(WriteIntentError::tostring)?;
        self.0.confirm_read(txn);
        Ok(())
    }

    pub fn check_read(&mut self, txnmap: &DbContext, txn: LockDataRef) -> Result<(), WriteIntentError> {
        self.check_write_intents(txnmap, txn)?;
        self.0.check_read(&txnmap.transaction_map, txn).map_err(|a| WriteIntentError::Other(a))?;
        self.0.confirm_read(txn);
        Ok(())
    }
}
