use std::collections::HashMap;

use clap::Command;
use env_logger::Env;

pub mod cmd_setup;
pub mod cmd;
pub mod dispatch;
pub mod helpers;
pub mod parse_and_run;
pub mod parse;
pub mod run;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(Env::default());

    let cmds: Vec<Box<dyn cmd::RunCmd>> = vec![
        Box::new(cmd::InitCmd),
        Box::new(cmd::AddCmd),
    ];

    let mut command = Command::new("oxen")
        .version(liboxen::constants::OXEN_VERSION)
        .about("🐂 is a machine learning dataset management toolchain")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(cmd_setup::branch())
        .subcommand(cmd_setup::checkout())
        .subcommand(cmd_setup::clone())
        .subcommand(cmd_setup::commit_cache())
        .subcommand(cmd_setup::commit())
        .subcommand(cmd_setup::config())
        .subcommand(cmd_setup::create_remote())
        .subcommand(cmd_setup::df())
        .subcommand(cmd_setup::diff())
        .subcommand(cmd_setup::download())
        .subcommand(cmd_setup::info())
        .subcommand(cmd_setup::inspect_kv_db())
        .subcommand(cmd_setup::fetch())
        .subcommand(cmd_setup::load())
        .subcommand(cmd_setup::log())
        .subcommand(cmd_setup::merge())
        .subcommand(cmd_setup::migrate())
        .subcommand(cmd_setup::pull())
        .subcommand(cmd_setup::push())
        .subcommand(cmd_setup::read_lines())
        .subcommand(cmd_setup::remote())
        .subcommand(cmd_setup::restore())
        .subcommand(cmd_setup::rm())
        .subcommand(cmd_setup::save())
        .subcommand(cmd_setup::schemas())
        .subcommand(cmd_setup::status())
        .subcommand(cmd_setup::upload());

    // Add all the commands to the command line
    let mut runners: HashMap<String, Box<dyn cmd::RunCmd>> = HashMap::new();
    for cmd in cmds {
        command = command.subcommand(cmd.args());
        runners.insert(cmd.name().to_string(), cmd);
    }

    // Parse the command line args and run the appropriate command
    let matches = command.get_matches();
    match matches.subcommand() {
        Some((cmd_setup::BRANCH, sub_matches)) => parse_and_run::branch(sub_matches).await,
        Some((cmd_setup::CHECKOUT, sub_matches)) => parse_and_run::checkout(sub_matches).await,
        Some((cmd_setup::CLONE, sub_matches)) => parse_and_run::clone(sub_matches).await,
        Some((cmd_setup::COMMIT_CACHE, sub_matches)) => {
            parse_and_run::compute_commit_cache(sub_matches).await
        }
        Some((cmd_setup::COMMIT, sub_matches)) => parse_and_run::commit(sub_matches).await,
        Some((cmd_setup::DIFF, sub_matches)) => parse_and_run::diff(sub_matches).await,
        Some((cmd_setup::CONFIG, sub_matches)) => parse_and_run::config(sub_matches),
        Some((cmd_setup::CREATE_REMOTE, sub_matches)) => {
            parse_and_run::create_remote(sub_matches).await
        }
        Some((cmd_setup::DF, sub_matches)) => parse_and_run::df(sub_matches),
        Some((cmd_setup::DOWNLOAD, sub_matches)) => parse_and_run::download(sub_matches).await,
        Some((cmd_setup::INFO, sub_matches)) => parse_and_run::info(sub_matches),
        Some((cmd_setup::KVDB_INSPECT, sub_matches)) => parse_and_run::kvdb_inspect(sub_matches),
        Some((cmd_setup::FETCH, sub_matches)) => parse_and_run::fetch(sub_matches).await,
        Some((cmd_setup::LOAD, sub_matches)) => parse_and_run::load(sub_matches).await,
        Some((cmd_setup::LOG, sub_matches)) => parse_and_run::log(sub_matches).await,
        Some((cmd_setup::MERGE, sub_matches)) => parse_and_run::merge(sub_matches),
        Some((cmd_setup::MIGRATE, sub_matches)) => parse_and_run::migrate(sub_matches).await,
        Some((cmd_setup::PULL, sub_matches)) => parse_and_run::pull(sub_matches).await,
        Some((cmd_setup::PUSH, sub_matches)) => parse_and_run::push(sub_matches).await,
        Some((cmd_setup::READ_LINES, sub_matches)) => parse_and_run::read_lines(sub_matches),
        Some((cmd_setup::REMOTE, sub_matches)) => parse_and_run::remote(sub_matches).await,
        Some((cmd_setup::RESTORE, sub_matches)) => parse_and_run::restore(sub_matches).await,
        Some((cmd_setup::RM, sub_matches)) => parse_and_run::rm(sub_matches).await,
        Some((cmd_setup::SAVE, sub_matches)) => parse_and_run::save(sub_matches).await,
        Some((cmd_setup::SCHEMAS, sub_matches)) => parse_and_run::schemas(sub_matches),
        Some((cmd_setup::STATUS, sub_matches)) => parse::status(sub_matches).await,
        Some((cmd_setup::UPLOAD, sub_matches)) => parse_and_run::upload(sub_matches).await,
        // TODO: Get these in the help command instead of just falling back
        Some((command, args)) => {
            // Lookup command in runners and run on args
            if let Some(runner) = runners.get(command) {
                match runner.run(args).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}");
                    }
                }
            } else {
                eprintln!("Unknown command `oxen {command}`");
            }
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachable!()
    }
}
