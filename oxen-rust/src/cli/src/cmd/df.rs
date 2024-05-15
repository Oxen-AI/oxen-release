use std::path::PathBuf;
use std::str::FromStr;

use async_trait::async_trait;
use clap::{arg, Arg, ArgMatches, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::ContentType;

use crate::cmd::RunCmd;

pub const NAME: &str = "df";
pub struct DFCmd;

#[async_trait]
impl RunCmd for DFCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
        .about("View and transform data frames. Supported types: csv, tsv, ndjson, jsonl, parquet.")
        .arg(arg!(<DF_SPEC> ... "The DataFrame you want to process. If in the schema subcommand the schema ref."))
        .arg_required_else_help(true)
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .help("Output file to store the transformed data")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("delimiter")
                .long("delimiter")
                .short('d')
                .help("The delimiter to use when reading the file. Default is ','")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("columns")
                .long("columns")
                .short('c')
                .help("A comma separated set of columns names to look at. Ex file,x,y")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("item")
                .long("item")
                .help("Select a specific row item from column to view it fully. Format: 'column:idx' ie: 'my_col_name:3'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("row")
                .long("row")
                .help("Select a specific row to view it fully. Format: '3'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("vstack")
                .long("vstack")
                .help("Combine row data from different files. The number of columns must match.")
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("slice")
                .long("slice")
                .help("A continuous slice of the data you want to look at. Format: 'start..end' Ex) '10..25' will take 15 elements, starting at 10 and ending at 25.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("head")
                .long("head")
                .help("Grab the first N entries of the data frame.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("tail")
                .long("tail")
                .help("Grab the last N entries of the data frame.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("take")
                .long("take")
                .short('t')
                .help("A comma separated set of row indices to look at. Ex 1,22,313")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("sort")
                .long("sort")
                .short('s')
                .help("Sort the output by a column name. Is run at the end of all the other transforms.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("sql")
                .long("sql")
                .help("Run a sql query on the data frame.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("text2sql")
                .long("text2sql")
                .help("Run a text query that translates to sql on the data frame.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("What remote host to run the query against. Ie: hub.oxen.ai")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("randomize")
                .long("randomize")
                .help("Randomize the order of the table")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("reverse")
                .long("reverse")
                .help("Reverse the order of the table")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("schema")
                .long("schema")
                .help("Print the full list of columns and data types within the schema in a dataframe.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("schema-flat")
                .long("schema-flat")
                .help("Print the full list of columns and data types within the schema.")
                .action(clap::ArgAction::SetTrue),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let opts = DFCmd::parse_df_args(args);
        let path = args.get_one::<String>("DF_SPEC").expect("required");
        if args.get_flag("schema") || args.get_flag("schema-flat") {
            let flatten = args.get_flag("schema-flat");
            let result = command::df::schema(path, flatten, opts)?;
            println!("{result}");
        } else {
            command::df(path, opts)?;
        }

        Ok(())
    }
}

impl DFCmd {
    pub fn parse_df_args(args: &ArgMatches) -> liboxen::opts::DFOpts {
        let vstack: Option<Vec<PathBuf>> = if let Some(vstack) = args.get_many::<String>("vstack") {
            let values: Vec<PathBuf> = vstack.map(std::path::PathBuf::from).collect();
            Some(values)
        } else {
            None
        };

        let mut content_type = "json";
        let maybe_content_type = args.get_one::<String>("content-type");
        if let Some(c) = maybe_content_type {
            content_type = c;
        }

        liboxen::opts::DFOpts {
            output: args
                .get_one::<String>("output")
                .map(std::path::PathBuf::from),
            delimiter: args.get_one::<String>("delimiter").map(String::from),
            slice: args.get_one::<String>("slice").map(String::from),
            page_size: args
                .get_one::<String>("page-size")
                .map(|x| x.parse::<usize>().expect("page-size must be valid int")),
            page: args
                .get_one::<String>("page")
                .map(|x| x.parse::<usize>().expect("page must be valid int")),
            head: args
                .get_one::<String>("head")
                .map(|x| x.parse::<usize>().expect("head must be valid int")),
            tail: args
                .get_one::<String>("tail")
                .map(|x| x.parse::<usize>().expect("tail must be valid int")),
            row: args
                .get_one::<String>("row")
                .map(|x| x.parse::<usize>().expect("row must be valid int")),
            take: args.get_one::<String>("take").map(String::from),
            columns: args.get_one::<String>("columns").map(String::from),
            filter: args.get_one::<String>("filter").map(String::from),
            aggregate: args.get_one::<String>("aggregate").map(String::from),
            col_at: args.get_one::<String>("col-at").map(String::from),
            vstack,
            index: args.get_flag("index"),
            add_col: args.get_one::<String>("add-col").map(String::from),
            add_row: args.get_one::<String>("add-row").map(String::from),
            get_row: args.get_one::<String>("get-row").map(String::from),
            delete_row: args.get_one::<String>("delete-row").map(String::from),
            sort_by: args.get_one::<String>("sort").map(String::from),
            sql: args.get_one::<String>("sql").map(String::from),
            text2sql: args.get_one::<String>("text2sql").map(String::from),
            host: args.get_one::<String>("host").map(String::from),
            unique: args.get_one::<String>("unique").map(String::from),
            content_type: ContentType::from_str(content_type).unwrap(),
            should_randomize: args.get_flag("randomize"),
            should_reverse: args.get_flag("reverse"),
            committed: args.get_flag("committed"),
        }
    }
}
