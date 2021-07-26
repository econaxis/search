pub fn discard_while<T: AsRef<[u8]> + ?Sized>(
    str: &T,
    filter: impl Fn(u8) -> bool,
) -> Result<&str, &str> {
    let str = str.as_ref();
    if str.len() == 0 {
        Err("String doesn't contain any matching")
    } else {
        if filter(str[0]) {
            Ok(std::str::from_utf8(&str[1usize..]).unwrap())
        } else {
            discard_while(&str[1..], filter)
        }
    }
}

pub fn take_while<T: AsRef<[u8]> + ?Sized>(
    str: &T,
    filter: impl Fn(u8) -> bool,
) -> Result<&str, &str> {
    let len_notincl = discard_while(str, filter)?.len();
    let len_incl = str.as_ref().len() - len_notincl;
    Ok(std::str::from_utf8(&str.as_ref()[0..len_incl]).unwrap())
}
pub fn take_to_delimiter<T: AsRef<[u8]> + ?Sized>(str: &T, delimiter: u8) -> Result<&str, &str> {
    let filter = |a| a == delimiter;

    let str = take_while(str, filter)?;
    let len = str.len();

    Ok(&str[0..len - 1])
}

#[cfg(test)]
mod tests {
    use crate::parsing::*;

    #[test]
    fn discardwhileworks() {
        assert_eq!(
            discard_while("fdsafdsa?fvcx".as_bytes(), |c| c == b'?').unwrap(),
            "fvcx"
        );
    }

    #[test]
    fn discardwhileerrors() {
        assert!(discard_while("fdsafdsafdsvc", |c| c == b'?').is_err())
    }

    #[test]
    fn take_while_test() {
        assert_eq!(
            take_while("fdsafdsa?fvcx", |c| c == b'?').unwrap(),
            "fdsafdsa?"
        );
    }

    #[test]
    fn take_to_delimiter_test() {
        assert_eq!(take_to_delimiter("fdsa?428dsvc", b'?'), Ok("fdsa"))
    }
}
