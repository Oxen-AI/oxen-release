/// # Aggregations
/// ('group_by_col') -> (list('col_1'), min('col_2'), n_unique('col_3'))
use crate::error::OxenError;

use nom::{
    bytes::complete::is_not,
    bytes::complete::tag,
    bytes::complete::take_while,
    character::complete::char,
    sequence::{delimited, separated_pair},
    IResult,
};

#[derive(Clone, Debug)]
pub enum DFAggFnType {
    List,
    Count,
    NUnique,
    Min,
    Max,
    ArgMin,
    ArgMax,
    Mean,
    Median,
    Std,
    Var,
    First,
    Last,
    Head,
    Tail,
    Unknown,
}

impl DFAggFnType {
    pub fn from_fn_name(s: &str) -> DFAggFnType {
        match s {
            "list" => DFAggFnType::List,
            "count" => DFAggFnType::Count,
            "n_unique" => DFAggFnType::NUnique,
            "min" => DFAggFnType::Min,
            "max" => DFAggFnType::Max,
            "arg_min" => DFAggFnType::ArgMin,
            "arg_max" => DFAggFnType::ArgMax,
            "mean" => DFAggFnType::Mean,
            "median" => DFAggFnType::Median,
            "std" => DFAggFnType::Std,
            "var" => DFAggFnType::Var,
            "first" => DFAggFnType::First,
            "last" => DFAggFnType::Last,
            "head" => DFAggFnType::Head,
            "tail" => DFAggFnType::Tail,
            _ => DFAggFnType::Unknown,
        }
    }
}

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
    c == '\'' || c == ' '
}

fn is_comma_or_whitespace(c: char) -> bool {
    c == ',' || c == ' '
}

fn is_open_paren(c: char) -> bool {
    c == '('
}

fn is_valid_fn_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn contained_in_single_quotes(input: &str) -> Result<(&str, &str), OxenError> {
    let result: IResult<&str, &str> = delimited(
        take_while(is_single_quote_or_whitespace),
        is_not("'"),
        char('\''),
    )(input);
    match result {
        Ok(result) => Ok(result),
        _ => Err(OxenError::parse_error(input)),
    }
}

fn contained_in_parens(input: &str) -> Result<(&str, &str), OxenError> {
    let result: IResult<&str, &str> = delimited(char('('), is_not(")"), char(')'))(input);
    match result {
        Ok(result) => Ok(result),
        _ => Err(OxenError::parse_error(input)),
    }
}

fn split_on_arrow(input: &str) -> Result<(&str, (&str, &str)), OxenError> {
    let result: IResult<&str, (&str, &str)> = separated_pair(
        take_while(is_whitespace),
        tag("->"),
        take_while(is_whitespace),
    )(input);
    match result {
        Ok(result) => Ok(result),
        _ => Err(OxenError::parse_error(input)),
    }
}

fn take_open_paren(input: &str) -> Result<(&str, &str), OxenError> {
    let result: IResult<&str, &str> = take_while(is_open_paren)(input);
    match result {
        Ok(result) => Ok(result),
        _ => Err(OxenError::parse_error(input)),
    }
}

fn take_fn_name(input: &str) -> Result<(&str, &str), OxenError> {
    let result: IResult<&str, &str> = take_while(is_valid_fn_char)(input);
    match result {
        Ok(result) => Ok(result),
        _ => Err(OxenError::parse_error(input)),
    }
}

fn take_comma(input: &str) -> Result<(&str, &str), OxenError> {
    let result: IResult<&str, &str> = take_while(is_comma_or_whitespace)(input);
    match result {
        Ok(result) => Ok(result),
        _ => Err(OxenError::parse_error(input)),
    }
}

pub fn parse_query(input: &str) -> Result<DFAggregation, OxenError> {
    log::debug!("GOT input: {}", input);
    // ('col_0', 'col_1') -> (list('col_3'), max('col_2'), n_unique('col_2'))
    let (remaining, parsed) = contained_in_parens(input)?;
    log::debug!(
        "contained_in_parens remaining: {}, parsed: {}",
        remaining,
        parsed
    );

    // parsed: 'col_0', 'col_1'
    let mut first_args: Vec<String> = vec![];
    for s in parsed.split(',') {
        let (_, parsed) = contained_in_single_quotes(s)?;
        log::debug!(
            "contained_in_single_quotes remaining: {}, parsed: {}",
            remaining,
            parsed
        );
        first_args.push(parsed.to_string());
    }

    // remaining: -> (list('col_3'), max('col_2'), n_unique('col_2'))
    let (remaining, parsed) = split_on_arrow(remaining)?;
    log::debug!(
        "split_on_arrow remaining: {}, parsed: {:?}",
        remaining,
        parsed
    );

    // remaining: (list('col_3'), max('col_2'), n_unique('col_2'))
    let (remaining, parsed) = take_open_paren(remaining)?;
    log::debug!(
        "take_open_paren remaining: {}, parsed: {}",
        remaining,
        parsed
    );

    // remaining: list('col_3'), max('col_2'), n_unique('col_2'))
    let mut agg_fns: Vec<DFAggFn> = vec![];
    let mut result = remaining;
    while result != ")" {
        log::debug!("START result {}", result);
        // result: , max('col_2'), n_unique('col_2'))
        if result.starts_with(',') {
            (result, _) = take_comma(result)?;
        }

        // result: max('col_2'), n_unique('col_2'))
        log::debug!("take_comma result {}", result);
        let (remaining, parsed) = take_fn_name(result)?;
        log::debug!(
            "take_alphanumeric remaining: {}, parsed: {}",
            remaining,
            parsed
        );
        let fn_name = parsed;

        // remaining: ('col_2'), n_unique('col_2'))
        let (remaining, parsed) = contained_in_parens(remaining)?;
        log::debug!(
            "contained_in_parens remaining: {}, parsed: {}",
            remaining,
            parsed
        );

        // parsed: 'col_2'
        let (_, parsed) = contained_in_single_quotes(parsed)?;
        log::debug!(
            "contained_in_single_quotes remaining: {}, parsed: {}",
            remaining,
            parsed
        );
        let arg = parsed;

        agg_fns.push(DFAggFn {
            name: String::from(fn_name),
            args: vec![String::from(arg)],
        });
        result = remaining;
    }

    log::debug!("GOT remaining: {:?}", remaining);

    Ok(DFAggregation {
        group_by: first_args,
        agg: agg_fns,
    })
}
