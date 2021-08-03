#[macro_export]
macro_rules! custom_error_impl {
    ($ty:ty) => {
        impl Into<String> for $ty {
            fn into(self) -> String {
                let mut buf = String::new();
                std::fmt::Write::write_fmt(&mut buf, format_args!("{:?}", self));
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