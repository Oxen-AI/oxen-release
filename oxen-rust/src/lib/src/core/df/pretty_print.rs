use crate::core::df::tabular;
/// # Pretty Print
/// The default Polars DataFrame print implementation puts characters
/// that do not fit well in our docs, so this transform helps cleanup the output.
///
/// ## Polars Example
/// ┌────────┬──────────┬───────────────────┐
/// │ prompt ┆ category ┆ .oxen.diff.status │
/// │ ---    ┆ ---      ┆ ---               │
/// │ str    ┆ str      ┆ str               │
/// ╞════════╪══════════╪═══════════════════╡
/// │ 20*20  ┆ math     ┆ added             │
/// └────────┴──────────┴───────────────────┘
///
/// ## Oxen Example
/// +--------+----------+-------------------+
/// | prompt ┆ category ┆ .oxen.diff.status |
/// | ---    ┆ ---      ┆ ---               |
/// | str    ┆ str      ┆ str               |
/// +--------+----------+-------------------+
/// | 20*20  ┆ math     ┆ added             |
/// +--------+----------+-------------------+
use polars::prelude::*;

pub fn df_to_str(df: &DataFrame) -> String {
    let default_fmt = format!("{:?}", df);

    default_fmt
        .replace(['┌', '└', '┬', '┴', '┐', '┘'], "+")
        .replace('─', "-")
        .replace('│', "|")
        .replace(['╞', '╡'], "+")
        .replace('═', "-")
        .replace('╪', "+")
        .replace('┆', "|")
}

pub fn df_to_str_full(df: &DataFrame) -> String {
    let mut height = df.height();
    let mut pos = 0;
    let mut ret_string = format!("shape: {:?}", df.shape());
    ret_string.push('\n');

    let first_slice = tabular::slice_df(df.clone(), pos, pos + 10);
    let first_fmt = format!("{:?}", first_slice);
    let mut first_lines = first_fmt.lines();
    first_lines.next();

    for _ in 0..15 {
        ret_string.push_str(first_lines.next().unwrap());
        ret_string.push('\n');
    }

    pos += 10;
    height -= 10;

    while height > 10 {
        ret_string.push_str("+----------+---------------------------------+");
        ret_string.push('\n');

        let mid_slice = tabular::slice_df(df.clone(), pos, pos + 10);
        let mid_fmt = format!("{:?}", mid_slice);
        let mut lines = mid_fmt.lines();

        for _ in 0..6 {
            lines.next();
        }

        for _ in 0..10 {
            ret_string.push_str(lines.next().unwrap());
            ret_string.push('\n');
        }

        pos += 10;
        height -= 10;
    }

    ret_string.push_str("+----------+---------------------------------+");
    ret_string.push('\n');

    let last_slice = tabular::slice_df(df.clone(), pos, pos + height);
    let last_fmt = format!("{:?}", last_slice);
    let mut last_lines = last_fmt.lines();

    for _ in 0..6 {
        last_lines.next();
    }

    for _ in 0..height {
        ret_string.push_str(last_lines.next().unwrap());
        ret_string.push('\n');
    }
    ret_string.push_str("+----------+---------------------------------+");

    ret_string
        .replace(['┌', '└', '┬', '┴', '┐', '┘'], "+")
        .replace('─', "-")
        .replace('│', "|")
        .replace(['╞', '╡'], "+")
        .replace('═', "-")
        .replace('╪', "+")
        .replace('┆', "|")
}
