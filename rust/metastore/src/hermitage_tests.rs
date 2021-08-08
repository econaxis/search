// tests stolen from https://github.com/ept/hermitage/blob/master/cockroachdb.md



use crate::rwtransaction_wrapper::DBTransaction;

#[test]
pub fn g1ctest() {
    let db = db!("1" = "10", "2" = "20");
    let mut txn1 = DBTransaction::new(&db);
    let mut txn2 = DBTransaction::new(&db);

    txn1.write(&"1".into(), "11".into()).unwrap();
    txn2.write(&"2".into(), "21".into()).unwrap();

    assert_matches!(txn2.read(&"1".into()), Err(..));
    assert_matches!(txn1.read(&"2".into()), Ok(..));
}
