#[cfg(test)]
pub mod tests {
    use crate::wal_watcher::WalLoader;

    use crate::db_context::create_empty_context;
    use crate::replicated_slave::SelfContainedDb;
    use crate::rwtransaction_wrapper::{auto_commit, ReplicatedTxn};
    use crate::timestamp::Timestamp;
    use crate::wal_watcher::wal_check_consistency::check_func1;
    use crate::DbContext;
    use rand::Rng;

    fn check(db: &DbContext) {
        let db2 = create_empty_context();

        db.wallog.apply(&db2);

        let s = SelfContainedDb::new(db2);

        check_func1(db, &s, Timestamp::now()).unwrap();

        std::mem::forget(s.db);
    }

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
        assert_eq!(
            auto_commit::read(&db, &"a".into()).unwrap().into_inner().1,
            "v1".into()
        );

        auto_commit::write(&db, &"a".into(), "v2".into());
        assert_eq!(
            auto_commit::read(&db, &"a".into()).unwrap().into_inner().1,
            "v2".into()
        );
        check(&db);
    }

    #[test]
    fn test_random() {
        // Generates random transactions and tests we can recover from our initial state just from using the WAL.
        use rand::thread_rng;
        let mut r = thread_rng();
        let db = db!();
        let mut txn = ReplicatedTxn::new(&db);

        for _ in 0..5000 {
            let decider = r.gen::<u64>();
            if decider % 20 == 0 {
                txn.abort();
                txn = ReplicatedTxn::new(&db);
            } else if decider % 20 == 1 {
                txn.commit();
                txn = ReplicatedTxn::new(&db);
            }

            let randv = r.gen::<u8>() % 20;
            let randv = randv.to_string();

            txn.write(&randv.clone().into(), randv.into());
        }
        txn.commit();

        check(&db);
    }
}
