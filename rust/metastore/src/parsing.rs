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

#[derive(PartialEq, Debug)]
enum Lexeme {
    LParens,
    RParens,
    Plus,
    Multiply,
    Number(u64),
}


type expr1 = Box<Ast>;

#[derive(Debug)]
enum Ast {
    Number(u64),
    Add(expr1, expr1),
    Multiply(expr1, expr1),
    Epsilon,
}

use Lexeme::*;
use std::fmt::{Debug, Formatter};
use crate::parsing::Op::Equals;

type L<'a> = &'a [Lexeme];


fn parse_number(l: L) -> (u64, L) {
    (match l[0] {
        Number(a) => a,
        _ => unreachable!()
    }, &l[1..])
}

fn parse_rest(l: L, before: Ast) -> (Ast, L) {
    if l.len() == 0 {
        return (before, l);
    }

    let lookahead = &l[0];

    match lookahead {
        x @ Plus | x @ Lexeme::Multiply => {
            let (ast0, rest) = parse_expression(&l[1..]);
            let (ast, rest) = parse_rest(rest, ast0);
            if x == &Plus {
                (Ast::Add(before.into(), ast.into()), rest)
            } else {
                (Ast::Multiply(before.into(), ast.into()), rest)
            }
        }
        _ => {
            (before, l)
        }
    }
}


fn match_t(l: L) -> (Ast, L) {
    if l.len() == 0 {
        return (Ast::Epsilon, l);
    }

    match &l[0] {
        LParens => {
            let rest = parse_expression(&l[1..]);
            assert_eq!(&rest.1[0], &RParens);
            (rest.0, &rest.1[1..])
        }
        RParens => { unreachable!() }
        _ => {
            let (num, rest) = parse_number(l);
            (Ast::Number(num), rest)
        }
    }
}

fn parse_expression(l: &[Lexeme]) -> (Ast, L) {
    let l = match_t(l);
    parse_rest(l.1, l.0)
}

#[derive(Debug)]
enum Op {
    Equals(ColumnExpr, ColumnExpr),
    Gt(ColumnExpr, ColumnExpr),
    Lt(ColumnExpr, ColumnExpr),
}

#[derive(Debug)]
enum BooleanOp {
    And(Sqlb, Sqlb),
    Or(Sqlb, Sqlb),
    Not(Sqlb),
}

type Sqlb = Box<Expr>;

enum ColumnExpr {
    Add(Box<Self>, Box<Self>),
    Multiply(Box<Self>, Box<Self>),
    CastExpr(CastExpr, Box<Self>),
    String(String),
    Number(u64),
}

#[derive(Debug)]
enum CastExpr {
    Int,
    Bool,
}

type ColumnExpr1 = Box<ColumnExpr>;

impl Debug for ColumnExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnExpr::Add(a, b) => f.write_fmt(format_args!("Add({:?}, {:?})", a, b)),
            ColumnExpr::Multiply(a, b) => f.write_fmt(format_args!("Multiply({:?}, {:?})", a, b)),
            ColumnExpr::String(s) => f.write_str(s),
            ColumnExpr::Number(a) => f.write_fmt(format_args!("{}", a)),
            ColumnExpr::CastExpr(a, b) => f.write_fmt(format_args!("{:?}({:?})", &a, b)),
        }
    }
}

#[derive(Debug)]
enum Expr {
    BooleanOp(BooleanOp),
    Column(ColumnExpr),
    Op(Op),
}

#[derive(PartialEq, Debug)]
enum Tokens {
    EqualsEquals,
    Number(u64),
    String(String),
    LParens,
    RParens,
    Gt,
    Lt,
    Plus,
    Multiply,
    Comma,
    Select,
    Where,
    From,
}

impl Tokens {
    fn str(a: &str) -> Tokens {
        Tokens::String(a.to_string())
    }
}

type Tok<'a> = &'a [Tokens];


/*

Expr = Expr "AND" Expr
     | Expr "OR" Expr
     | ( Expr )

Expr = ColumnExpr '==' ColumnExpr
     | ColumnExpr' '<' ColumnExpr

ColumnExpr = String
           | Number
           | ColumnExpr
           | CastExpr

ColumnExpr = ColumnExpr Arest;

Arest = ('+' | '-') ColumnExpr Arest | epsilon;

ArithmeticTerm = String
               | Number
               | ( ColumnExpr );
 */

fn parse_expr(t: Tok) -> (Expr, Tok) {
    let mut remaining;
    let expr = match &t[0] {
        Tokens::LParens => {
            let (expr, next) = parse_expr(&t[1..]);
            remaining = &next[1..];
            assert_eq!(next[0], Tokens::RParens);
            expr
        }
        _ => {
            let (left, t) = parse_column_expr(t);
            assert_matches!(t[0], Tokens::Gt | Tokens::Lt | Tokens::EqualsEquals);
            let (right, rem) = parse_column_expr(&t[1..]);
            remaining = rem;
            Expr::Op(match &t[0] {
                Tokens::Gt => Op::Gt(left, right),
                Tokens::Lt => Op::Lt(left, right),
                Tokens::EqualsEquals => Op::Equals(left, right),
                _ => unreachable!()
            })
        }
    };

    (expr, remaining)
}

#[test]
fn test_parse_expr() {
    use Tokens::*;
    dbg!(parse_expr(&[Tokens::str("test"), Multiply, Tokens::str("telephone"), EqualsEquals, Tokens::str("id"), Plus, Number(1), Multiply, Number(10)]));
}

fn parse_arithmetic_term(t: Tok) -> (ColumnExpr, Tok) {
    match &t[0] {
        Tokens::String(str) => (ColumnExpr::String(str.to_string()), &t[1..]),
        Tokens::Number(num) => (ColumnExpr::Number(*num), &t[1..]),
        Tokens::LParens => {
            let (expr, rest) = parse_column_expr(&t[1..]);
            assert_eq!(rest[0], Tokens::RParens);
            (expr, &rest[1..])
        }
        _ => { unreachable!() }
    }
}


fn parse_cast(t: Tok) -> (ColumnExpr, Tok) {
    let str = match &t[0] {
        Tokens::String(str) => str,
        _ => unreachable!()
    };
    assert_eq!(t[1], Tokens::LParens);
    let (column, rest) = parse_column_expr(&t[2..]);
    assert_eq!(rest[0], Tokens::RParens);
    match str.as_str() {
        "int" => (ColumnExpr::CastExpr(CastExpr::Int, column.into()), &rest[1..]),
        "bool" => (ColumnExpr::CastExpr(CastExpr::Bool, column.into()), &rest[1..]),
        _ => unreachable!()
    }
}

fn parse_column_expr(t: Tok) -> (ColumnExpr, Tok) {
    fn parse_arithmetic_rest(t: Tok, before: ColumnExpr) -> (ColumnExpr, Tok) {
        if t.len() == 0 {
            return (before, t);
        }
        let (term, rest) = match &t[0] {
            Tokens::Plus => {
                let (term, rest) = parse_arithmetic_term(&t[1..]);
                (ColumnExpr::Add(before.into(), term.into()), rest)
            }
            Tokens::Multiply => {
                let (term, rest) = parse_arithmetic_term(&t[1..]);
                (ColumnExpr::Multiply(before.into(), term.into()), rest)
            }
            _ => {
                return (before, t);
            }
        };
        let after = parse_arithmetic_rest(rest, term);
        after
    }
    match &t[0] {
        Tokens::String(str) if t.get(1).map(|a| a == &Tokens::LParens).unwrap_or(false) => {
            let (a, b) = parse_cast(&t[0..]);
            (a, b)
        }
        _ => {
            let (t1, t2) = parse_arithmetic_term(t);
            let (t1, t2) = parse_arithmetic_rest(t2, t1);
            (t1, t2)
        }
    }
}

fn match_or(t: Tok, a: Tokens) -> (bool, Tok) {
    if t[0] == a {
        (true, &t[1..])
    } else {
        (false, t)
    }
}

fn match1(t: Tok, a: Tokens) -> Tok {
    assert_eq!(t[0], a);
    &t[1..]
}


fn parse_column_list(t: Tok) -> (Vec<ColumnExpr>, Tok) {
    let (first, rest) = parse_column_expr(t);
    let mut columns = vec![first];
    fn match_rest<'a>(t: Tok<'a>, columns: &mut Vec<ColumnExpr>) -> Tok<'a> {
        if t.len() > 0 && t[0] == Tokens::Comma {
            let (next, rest) = parse_column_expr(&t[1..]);
            columns.push(next);
            return match_rest(rest, columns);
        }
        return t;
    }
    let t = match_rest(rest, &mut columns);
    (columns, t)
}


#[test]
fn test_lex() {
    let lexed = lex("SELECT int(id),tele, address FROM (SELECT number FROM subtable) WHERE 3 * ((id + 1) * tele) >= tele".to_string());

    dbg!(parse_select_stmt(&lexed));
}

#[derive(Debug)]
enum TableExpression {
    NamedTable(String),
    SelectQuery(SelectQuery),
}

#[derive(Debug)]
struct SelectQuery {
    distinct: bool,
    column_list: Vec<ColumnExpr>,
    where_exp: Option<Expr>,
    from: Box<TableExpression>
}

fn parse_table_expr(t: Tok) -> (TableExpression, Tok) {
    match &t[0] {
        Tokens::LParens => {
            let (select, t) = parse_select_stmt(&t[1..]);
            let t = match1(t, Tokens::RParens);
            (TableExpression::SelectQuery(select), t)
        }
        _ => {
            match &t[0] {
                Tokens::String(a) => (TableExpression::NamedTable(a.to_string()), &t[1..]),
                _ => unreachable!()
            }
        }
    }
}

fn parse_select_stmt(t: Tok) -> (SelectQuery, Tok) {
    let t = match1(t, Tokens::Select);
    let (distinct, t) = match_or(t, Tokens::str("DISTINCT"));
    let (list, t) = parse_column_list(t);

    let t = match1(t, Tokens::From);
    let (table_expression, t) = parse_table_expr(t);

    let (is_whered, mut t) = match_or(t, Tokens::Where);
    let where_exp = is_whered.then(|| {
        let (whereexp, t_) = parse_expr(t);
        t = t_;
        whereexp
    });

    println!("select columns {:?} from {:?} where {:?}", list, table_expression, where_exp);
    (SelectQuery {
        distinct,
        column_list: list,
        where_exp,
        from: table_expression.into()
    }, t)
}

#[test]
fn test_select() {
    use Tokens::*;
    let t1 = [Select, Tokens::str("int"), LParens, Tokens::str("id"), RParens, Comma, Tokens::str("tele"), Comma, Tokens::str("address"),
        From, LParens, Select, Tokens::str("int"), LParens, Tokens::str("id"), RParens, Comma, Tokens::str("tele"), Comma, Tokens::str("address"),
        From, Tokens::str("table_name"), RParens,
        Where, Tokens::str("id"), Plus, Number(1), EqualsEquals, Tokens::str("tele")];
    parse_select_stmt(&t1);
}

fn lex(s: String) -> Vec<Tokens> {
    use Tokens::*;
    let chars = ['(', ' ', ')', ','];
    let split = s.as_str().match_indices(&chars[..]);

    let mut prev_index = 0;
    let mut tokens: Vec<&str> = Vec::new();

    for (index, s1) in split {
        tokens.push(&s[prev_index..index]);
        tokens.push(s1);
        prev_index = index + 1;
    }
    tokens.push(&s[prev_index..]);
    let tokens: Vec<_> = tokens.drain_filter(|&mut a| !matches!(a, "" | " ")).collect();
    tokens.iter().map(|&a| match a {
        "SELECT" | "select" => Select,
        "FROM" | "from" => From,
        "WHERE" | "where" => Where,
        "(" => LParens,
        ")" => RParens,
        "," => Comma,
        "*" => Multiply,
        "+" => Plus,
        ">=" => Gt,
        "<=" => Lt,
        _ if a.chars().all(|c| c.is_numeric()) => Number(a.parse::<u64>().unwrap()),
        _ => {
            Tokens::str(a)
        }
    }).collect()
}

#[cfg(test)]
mod tests {
    use crate::parsing::*;

    #[test]
    fn test1() {
        use Tokens::*;
        dbg!(parse_column_expr(&[LParens, LParens, Number(3), Multiply, Number(3), RParens, Plus, Number(5), Plus, Number(7), RParens, Multiply, Number(100)]));
        dbg!(parse_column_expr(&[Tokens::str("int"), LParens, Tokens::str("idcol"), RParens]));
    }

    #[test]
    fn test_parse() {
        let expr = parse_expression(&[LParens, LParens, Number(3), Multiply, Number(3), RParens, Plus, Number(5), Plus, Number(7), RParens, Multiply, Number(100)]);
        println!("{:?}", expr);
    }


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
