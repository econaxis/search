#[macro_export]
macro_rules! retry5 {
    ($tree:tt) => {
        let mut counter = 0;
        loop {
            let res: Result<(), String> = try {
                $tree
                Ok(())
            };
            if res.is_ok() {
                break;
            } else if counter < 5 {
                return Err("Too many retries".to_string());
            } else {
                counter+=1;
            }
        };
    };
}