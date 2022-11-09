use crate::error::OxenError;

use nom::{
    bytes::complete::is_not,
    bytes::complete::tag,
    bytes::complete::take_while,
    character::complete::char,
    sequence::{delimited, separated_pair},
    IResult,
};

// Example agg:
// 'group_by' -> ('col_1', min('col_2'), n_unique('col_3'))
#[derive(Clone, Debug)]
pub struct DFAggFn {
    pub name: String,
    pub args: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct DFAggregation {
    pub group_by: Vec<String>,
    pub agg: Vec<DFAggFn>,
}

fn is_whitespace(c: char) -> bool {
    c == ' '
}

fn is_single_quote_or_whitespace(c: char) -> bool {
    c == ' ' || c == '\''
}

fn is_open_paren(c: char) -> bool {
    c == '('
}

fn is_alphanumeric(c: char) -> bool {
    c.is_alphanumeric()
}

fn contained_in_single_quotes(input: &str) -> IResult<&str, &str> {
    delimited(
        take_while(is_single_quote_or_whitespace),
        is_not("'"),
        char('\''),
    )(input)
}

fn contained_in_parens(input: &str) -> IResult<&str, &str> {
    delimited(char('('), is_not(")"), char(')'))(input)
}

fn split_on_arrow(input: &str) -> IResult<&str, (&str, &str)> {
    separated_pair(
        take_while(is_whitespace),
        tag("->"),
        take_while(is_whitespace),
    )(input)
}

fn take_open_paren(input: &str) -> IResult<&str, &str> {
    take_while(is_open_paren)(input)
}

fn take_alphanumeric(input: &str) -> IResult<&str, &str> {
    take_while(is_alphanumeric)(input)
}

pub fn parse_query(input: &str) -> Result<DFAggregation, OxenError> {
    log::debug!("GOT input: {}", input);
    // ('col_0', 'col_1') -> (list('col_3'), max('col_2'), n_unique('col_2'))
    let (remaining, parsed) = contained_in_parens(input).unwrap();
    log::debug!(
        "contained_in_parens remaining: {}, parsed: {}",
        remaining,
        parsed
    );

    // parsed: 'col_0', 'col_1'
    let mut first_args: Vec<String> = vec![];
    for s in parsed.split(',') {
        let (_, parsed) = contained_in_single_quotes(s).unwrap();
        log::debug!(
            "contained_in_single_quotes remaining: {}, parsed: {}",
            remaining,
            parsed
        );
        first_args.push(parsed.to_string());
    }

    let (remaining, parsed) = split_on_arrow(remaining).unwrap();
    log::debug!(
        "split_on_arrow remaining: {}, parsed: {:?}",
        remaining,
        parsed
    );

    let (remaining, parsed) = take_open_paren(remaining).unwrap();
    log::debug!(
        "take_open_paren remaining: {}, parsed: {}",
        remaining,
        parsed
    );

    let (remaining, parsed) = take_alphanumeric(remaining).unwrap();
    log::debug!(
        "take_alphanumeric remaining: {}, parsed: {}",
        remaining,
        parsed
    );
    let fn_name = parsed;

    let (remaining, parsed) = contained_in_parens(remaining).unwrap();
    log::debug!(
        "contained_in_parens remaining: {}, parsed: {}",
        remaining,
        parsed
    );

    let (remaining, parsed) = contained_in_single_quotes(parsed).unwrap();
    log::debug!(
        "contained_in_single_quotes remaining: {}, parsed: {}",
        remaining,
        parsed
    );
    let second_arg = parsed;

    log::debug!("GOT remaining: {:?}", remaining);

    Ok(DFAggregation {
        group_by: first_args,
        agg: vec![DFAggFn {
            name: String::from(fn_name),
            args: vec![String::from(second_arg)],
        }],
    })
}
