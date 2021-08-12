#[cfg(test)]
pub mod tests {
    use crate::wal_watcher::{ByteBufferWAL, WalLoader};
    use std::cell::RefCell;
    use std::sync::Mutex;
    use crate::rwtransaction_wrapper::{auto_commit, ReplicatedTxn};


    #[test]
    fn test1() {
        let db = db!();
        assert_matches!(auto_commit::read(&db, &"a".into()), Err(..));


        let mut aborter = ReplicatedTxn::new(&db);
        aborter.write(&"a".into(), "v".into());
        aborter.abort();
        assert_matches!(auto_commit::read(&db, &"a".into()), Err(..));
        assert_matches!(auto_commit::read(&db, &"a".into()), Err(..));

        auto_commit::write(&db, &"a".into(), "v1".into());
        assert_eq!(auto_commit::read(&db, &"a".into()).unwrap().into_inner().1, "v1".to_string());

        auto_commit::write(&db, &"a".into(), "v2".into());
        assert_eq!(auto_commit::read(&db, &"a".into()).unwrap().into_inner().1, "v2".to_string());

        let db2 = db!();

        db.wallog.apply(&db2);

        assert_eq!(db.db.printdb(), db2.db.printdb());
    }
}