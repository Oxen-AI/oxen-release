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
use crate::core::df::tabular; 

pub fn df_to_str(df: &DataFrame) -> String {

    let mut height = df.height(); 
    let mut pos = 0; 
    let mut ret_string = String::new(); 

    let first_slice = tabular::slice_df(df.clone(), pos, pos + 10);
    let first_fmt = format!("{:?}", first_slice);

    ret_string.push_str(&first_fmt);  
        
    pos += 10;
    height -= 10; 

    while height > 10 {
        let mid_slice = tabular::slice_df(df.clone(), pos, pos + 10);
        let mid_fmt = format!("{:?}", mid_slice);
        let mut lines = mid_fmt.lines();

        for _j in 0..6 {
            lines.next(); 
        }

        for _j in 0..10 {
            ret_string.push_str(lines.next().unwrap());
            ret_string.push_str("\n");  
        }

        pos += 10;
        height -= 10; 
    }

    let last_slice = tabular::slice_df(df.clone(), pos, pos + 10);
    let last_fmt = format!("{:?}", last_slice);
    ret_string.push_str(&last_fmt);  


    ret_string
        .replace(['┌', '└', '┬', '┴', '┐', '┘'], "+")
        .replace('─', "-")
        .replace('│', "|")
        .replace(['╞', '╡'], "+")
        .replace('═', "-")
        .replace('╪', "+")
        .replace('┆', "|")
}
