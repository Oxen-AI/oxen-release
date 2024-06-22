use std::collections::HashMap;
use std::process::ExitCode;

use clap::Command;
use env_logger::Env;

pub mod cmd;
pub mod cmd_setup;
pub mod dispatch;
pub mod helpers;
pub mod parse_and_run;

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init_from_env(Env::default());

    let cmds: Vec<Box<dyn cmd::RunCmd>> = vec![
        Box::new(cmd::AddCmd),
        Box::new(cmd::BranchCmd),
        Box::new(cmd::CheckoutCmd),
        Box::new(cmd::CloneCmd),
        Box::new(cmd::ConfigCmd),
        Box::new(cmd::CommitCmd),
        Box::new(cmd::CreateRemoteCmd),
        Box::new(cmd::DbCmd),
        Box::new(cmd::DFCmd),
        Box::new(cmd::DiffCmd),
        Box::new(cmd::InitCmd),
        Box::new(cmd::LogCmd),
        Box::new(cmd::MooCmd),
        Box::new(cmd::RestoreCmd),
        Box::new(cmd::ReadLinesCmd),
        Box::new(cmd::RmCmd),
        Box::new(cmd::SaveCmd),
        Box::new(cmd::SchemasCmd),
        Box::new(cmd::StatusCmd),
        Box::new(cmd::UploadCmd),
    ];

    let mut command = Command::new("oxen")
        .version(liboxen::constants::OXEN_VERSION)
        .about("🐂 is a machine learning dataset management toolchain")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);

    // Add all the commands to the command line
    let mut runners: HashMap<String, Box<dyn cmd::RunCmd>> = HashMap::new();
    for cmd in cmds {
        command = command.subcommand(cmd.args());
        runners.insert(cmd.name().to_string(), cmd);
    }

    // TODO: Refactor these into their own modules
    command = command
        .subcommand(cmd_setup::commit_cache())
        .subcommand(cmd_setup::download())
        .subcommand(cmd_setup::info())
        .subcommand(cmd_setup::fetch())
        .subcommand(cmd_setup::load())
        .subcommand(cmd_setup::merge())
        .subcommand(cmd_setup::migrate())
        .subcommand(cmd_setup::pull())
        .subcommand(cmd_setup::push())
        .subcommand(cmd_setup::remote());

    // Parse the command line args and run the appropriate command
    let matches = command.get_matches();
    match matches.subcommand() {
        Some((cmd_setup::COMMIT_CACHE, sub_matches)) => {
            parse_and_run::compute_commit_cache(sub_matches).await
        }
        Some((cmd_setup::DOWNLOAD, sub_matches)) => parse_and_run::download(sub_matches).await,
        Some((cmd_setup::INFO, sub_matches)) => parse_and_run::info(sub_matches),
        Some((cmd_setup::FETCH, sub_matches)) => parse_and_run::fetch(sub_matches).await,
        Some((cmd_setup::LOAD, sub_matches)) => parse_and_run::load(sub_matches).await,
        Some((cmd_setup::MERGE, sub_matches)) => parse_and_run::merge(sub_matches),
        Some((cmd_setup::MIGRATE, sub_matches)) => parse_and_run::migrate(sub_matches).await,
        Some((cmd_setup::PULL, sub_matches)) => parse_and_run::pull(sub_matches).await,
        Some((cmd_setup::PUSH, sub_matches)) => parse_and_run::push(sub_matches).await,
        Some((cmd_setup::REMOTE, sub_matches)) => parse_and_run::remote(sub_matches).await,
        // TODO: Get these in the help command instead of just falling back
        Some((command, args)) => {
            // Lookup command in runners and run on args
            if let Some(runner) = runners.get(command) {
                match runner.run(args).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}");
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                eprintln!("Unknown command `oxen {command}`");
                return ExitCode::FAILURE;
            }
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachable!()
    }

    ExitCode::SUCCESS
}
