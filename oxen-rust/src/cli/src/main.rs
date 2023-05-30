use clap::Command;
use env_logger::Env;

pub mod cmd_setup;
pub mod dispatch;
pub mod parse_and_run;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(Env::default());

    let command = Command::new("oxen")
        .version(liboxen::constants::OXEN_VERSION)
        .about("🐂 is a machine learning dataset management toolchain")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(cmd_setup::add())
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
        .subcommand(cmd_setup::init())
        .subcommand(cmd_setup::inspect_kv_db())
        .subcommand(cmd_setup::log())
        .subcommand(cmd_setup::merge())
        .subcommand(cmd_setup::pull())
        .subcommand(cmd_setup::push())
        .subcommand(cmd_setup::read_lines())
        .subcommand(cmd_setup::remote())
        .subcommand(cmd_setup::restore())
        .subcommand(cmd_setup::rm())
        .subcommand(cmd_setup::schemas())
        .subcommand(cmd_setup::status());

    let matches = command.get_matches();

    match matches.subcommand() {
        Some((cmd_setup::ADD, sub_matches)) => parse_and_run::add(sub_matches).await,
        Some((cmd_setup::BRANCH, sub_matches)) => parse_and_run::branch(sub_matches).await,
        Some((cmd_setup::CHECKOUT, sub_matches)) => parse_and_run::checkout(sub_matches).await,
        Some((cmd_setup::CLONE, sub_matches)) => parse_and_run::clone(sub_matches).await,
        Some((cmd_setup::COMMIT_CACHE, sub_matches)) => {
            parse_and_run::compute_commit_cache(sub_matches).await
        }
        Some((cmd_setup::COMMIT, sub_matches)) => parse_and_run::commit(sub_matches).await,
        Some((cmd_setup::CONFIG, sub_matches)) => parse_and_run::config(sub_matches),
        Some((cmd_setup::CREATE_REMOTE, sub_matches)) => {
            parse_and_run::create_remote(sub_matches).await
        }
        Some((cmd_setup::DF, sub_matches)) => parse_and_run::df(sub_matches),
        Some((cmd_setup::DIFF, sub_matches)) => parse_and_run::diff(sub_matches).await,
        Some((cmd_setup::INIT, sub_matches)) => parse_and_run::init(sub_matches).await,
        Some((cmd_setup::KVDB_INSPECT, sub_matches)) => parse_and_run::kvdb_inspect(sub_matches),
        Some((cmd_setup::LOG, sub_matches)) => parse_and_run::log(sub_matches).await,
        Some((cmd_setup::MERGE, sub_matches)) => parse_and_run::merge(sub_matches),
        Some((cmd_setup::PULL, sub_matches)) => parse_and_run::pull(sub_matches).await,
        Some((cmd_setup::PUSH, sub_matches)) => parse_and_run::push(sub_matches).await,
        Some((cmd_setup::READ_LINES, sub_matches)) => parse_and_run::read_lines(sub_matches),
        Some((cmd_setup::REMOTE, sub_matches)) => parse_and_run::remote(sub_matches).await,
        Some((cmd_setup::RESTORE, sub_matches)) => parse_and_run::restore(sub_matches).await,
        Some((cmd_setup::RM, sub_matches)) => parse_and_run::rm(sub_matches).await,
        Some((cmd_setup::SCHEMAS, sub_matches)) => parse_and_run::schemas(sub_matches),
        Some((cmd_setup::STATUS, sub_matches)) => parse_and_run::status(sub_matches).await,
        // TODO: Get these in the help command instead of just falling back
        Some((ext, _sub_matches)) => {
            println!("Unknown command {ext}");
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachable!()
    }
}
