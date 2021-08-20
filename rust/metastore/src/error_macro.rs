#[macro_export]
macro_rules! custom_error_impl {
    ($ty:ty) => {
        impl From<$ty> for String {
            fn from(a: $ty) -> String {
                let mut buf = String::new();
                std::fmt::Write::write_fmt(&mut buf, format_args!("{:?}", a)).unwrap();
                buf
            }
        }
        impl From<String> for $ty {
            fn from(a: String) -> Self {
                Self::Other(a)
            }
        }

        impl From<&str> for $ty {
            fn from(a: &str) -> Self {
                Self::Other(a.to_string())
            }
        }
    };
}