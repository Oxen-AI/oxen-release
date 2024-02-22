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
