use crate::create_empty_context;


#[macro_export]
macro_rules! db {
    (
        $(
            $key:literal = $value:literal
        ), *
    ) => {{
        let ctx = $crate::create_replicated_context();
        let mut txninit = $crate::rwtransaction_wrapper::DBTransaction::new(&ctx);

        $(
            txninit.write(&crate::object_path::ObjectPath::from($key), std::borrow::Cow::from($value)).unwrap();
        )*
        txninit.commit();

        ctx}};

}


mod tests {
    use crate::rwtransaction_wrapper::DBTransaction;

    #[test]
    fn test() {
        let db = db! {
            "a2" = "key2",
            "a3" = "key3",
            "a4" = "key4"
        };

        let mut txn = DBTransaction::new(&db);
        assert_eq!(txn.read(&"a2".into()).unwrap(), "key2".to_string());
        assert_eq!(txn.read(&"a3".into()).unwrap(), "key3".to_string());
        assert_eq!(txn.read(&"a4".into()).unwrap(), "key4".to_string());
    }
}
