#[cfg(test)]
mod tests {

    use quickcheck::{Arbitrary, Gen, TestResult};
    use quickcheck_macros::quickcheck;
    use serde_json::Value;

    use super::super::json_processing::{
        check_valid_json, json_to_map, map_to_json, PrimitiveValue,
    };
    use crate::create_empty_context;
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::DBTransaction;

    #[derive(Clone, Debug)]
    struct ArbJson(pub Value);

    #[derive(Clone, Debug)]
    struct ArbJson2(pub Value);

    impl ObjectPath {
        fn arbitrary(g: &mut Gen, range: &[u8]) -> Self {
            let numelems = u8::arbitrary(g) / 10;

            let mut v = Vec::with_capacity((numelems) as usize);
            v.push("/user".to_string());

            for u in range {
                v.push(u.to_string());
            }

            let mut v: String = v.join("/");
            v.push('/');

            ObjectPath::from(v)
        }
    }

    const COUNT: u8 = 4;

    fn permuteu8(max: u8) -> Vec<[u8; COUNT as usize]> {
        let numelems = max.pow(COUNT as u32);

        let mut ret = Vec::with_capacity(numelems as usize);
        ret.resize(numelems as usize, [0; COUNT as usize]);

        for i in 0..COUNT {
            let modulo = max.pow(i as u32);
            for j in 0..numelems {
                ret[j as usize][i as usize] = (j / modulo) % max;
            }
        }
        ret
    }

    impl Arbitrary for ArbJson {
        fn arbitrary(g: &mut Gen) -> Self {
            let allpaths = permuteu8(3);

            let mut map = Vec::with_capacity(allpaths.len());
            for p in allpaths {
                let mut str = String::arbitrary(g);
                map.push((ObjectPath::arbitrary(g, &p), PrimitiveValue::String(str)));
            }

            let mut value = map_to_json(&map);
            let value = value["user"].take();

            ArbJson(value)
        }
    }

    fn random_ascii_string(g: &mut Gen) -> String {
        String::arbitrary(g)
            .chars()
            .filter_map(|char| {
                if char.is_ascii_alphanumeric() {
                    Some(char)
                } else {
                    None
                }
            })
            .collect()
    }

    impl Arbitrary for ArbJson2 {
        fn arbitrary(g: &mut Gen) -> Self {
            fn inner(g: &mut Gen, rem: usize) -> Value {
                if rem < 5 && (u8::arbitrary(g) % 2 == 0 || rem == 0) {
                    Value::String(String::arbitrary(g))
                } else {
                    let mut ret = serde_json::Map::new();
                    for i in 0..u8::arbitrary(g) % 2 + 1 {
                        let mut str = i.to_string();
                        str.push_str(&*random_ascii_string(g));
                        ret.insert(str, inner(g, rem - 1));
                    }

                    Value::Object(ret)
                }
            }

            ArbJson2(inner(g, 10))
        }
    }

    fn truncate_string(str: &str, maxlen: usize) -> &str {
        let index = str
            .char_indices()
            .skip(maxlen)
            .find(|a| str.is_char_boundary(a.0));

        match index {
            Some(a) => &str[0..a.0],
            None => &str,
        }
    }

    #[quickcheck]
    fn test_arbjson1(ArbJson(v): ArbJson) -> TestResult {
        test_json(v)
    }

    #[quickcheck]
    fn test_arbjson2(ArbJson2(v): ArbJson2) -> TestResult {
        test_json(v)
    }

    fn test_json(v: Value) -> TestResult {
        if !check_valid_json(&v) {
            return TestResult::discard();
        }

        let ctx = create_empty_context();
        let mut txn = DBTransaction::new(&ctx);
        super::super::write_json(v.clone(), &mut txn);
        txn.commit();

        let value =
            crate::rwtransaction_wrapper::json_request_writers::read_json_request("/", &ctx);

        let map = json_to_map(v.clone());

        map.iter().for_each(|(path, _value)| {
            let new = path.as_str().strip_prefix("/user").unwrap();
            let _a =
                crate::rwtransaction_wrapper::json_request_writers::read_json_request(new, &ctx);
            let stripped = match new.strip_suffix('/') {
                Some(x) => x,
                None => new,
            };
            let _b = v.pointer(stripped).unwrap();
            assert_eq!(
                *v.pointer(stripped).unwrap(),
                crate::rwtransaction_wrapper::json_request_writers::read_json_request(new, &ctx)
            );
        });

        assert_eq!(value, v);
        let finaldb = serde_json::to_string_pretty(&value).unwrap();

        println!("Final db: {}", truncate_string(&finaldb, 500));

        TestResult::passed()
    }
}
