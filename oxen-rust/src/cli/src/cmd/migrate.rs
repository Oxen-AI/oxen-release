use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};
use liboxen::error::OxenError;
use std::path::Path;

use liboxen::command::migrate::{
    AddDirectoriesToCacheMigration, CacheDataFrameSizeMigration, CreateMerkleTreesMigration,
    Migrate, PropagateSchemasMigration, UpdateVersionFilesMigration,
};

use crate::cmd::RunCmd;
pub const NAME: &str = "migrate";
pub struct MigrateCmd;

#[async_trait]
impl RunCmd for MigrateCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // TODO: if future migration commands all are expected to follow the <path> --all structure,
        // move that arg parsing up to the top level of the command
        Command::new(NAME)
            .about("Run a named migration on a server repository or set of repositories")
            .subcommand_required(true)
            .subcommand(
                Command::new("up")
                    .about("Apply a named migration forward.")
                    .subcommand_required(true)
                    .subcommand(
                        Command::new(UpdateVersionFilesMigration.name())
                            .about("Migrates version files from commit id to common prefix")
                            .arg(
                                Arg::new("PATH")
                                    .help("Directory in which to apply the migration")
                                    .required(true),
                            )
                            .arg(
                                Arg::new("all")
                                    .long("all")
                                    .short('a')
                                    .help(
                                        "Run the migration for all oxen repositories in this directory",
                                    )
                                    .action(clap::ArgAction::SetTrue),
                            ),
                    )
                    .subcommand(
                        Command::new(PropagateSchemasMigration.name())
                            .about("Propagates schemas to the latest commit")
                            .arg(
                                Arg::new("PATH")
                                    .help("Directory in which to apply the migration")
                                    .required(true),
                            )
                            .arg(
                                Arg::new("all")
                                    .long("all")
                                    .short('a')
                                    .help(
                                        "Run the migration for all oxen repositories in this directory",
                                    )
                                    .action(clap::ArgAction::SetTrue),
                            ),
                    )
                    .subcommand(
                        Command::new(CacheDataFrameSizeMigration.name())
                            .about("Caches size for existing data frames")
                            .arg(
                                Arg::new("PATH")
                                    .help("Directory in which to apply the migration")
                                    .required(true),
                            )
                            .arg(
                                Arg::new("all")
                                    .long("all")
                                    .short('a')
                                    .help(
                                        "Run the migration for all oxen repositories in this directory",
                                    )
                                    .action(clap::ArgAction::SetTrue),
                            ),
                    )
                    .subcommand(
                        Command::new(CreateMerkleTreesMigration.name())
                        .about("Reformats the underlying data model into merkle trees for storage and lookup efficiency")
                        .arg(
                            Arg::new("PATH")
                                .help("Directory in which to apply the migration")
                                .required(true),
                        )
                        .arg(
                            Arg::new("all")
                                .long("all")
                                .short('a')
                                .help(
                                    "Run the migration for all oxen repositories in this directory",
                                )
                                .action(clap::ArgAction::SetTrue),
                        ),
                    )
                    .subcommand(
                        Command::new(AddDirectoriesToCacheMigration.name())
                        .about("SERVER ONLY: Re-caches past commits to include directories in the cache")
                        .arg(
                            Arg::new("PATH")
                                .help("Directory in which to apply the migration")
                                .required(true),
                        )
                        .arg(
                            Arg::new("all")
                                .long("all")
                                .short('a')
                                .help(
                                    "Run the migration for all oxen repositories in this directory",
                                )
                                .action(clap::ArgAction::SetTrue),
                        ),
                    )
            )
            .subcommand(
                Command::new("down")
                    .about("Apply a named migration backward.")
                    .subcommand_required(true)
                    .subcommand(
                        Command::new(CacheDataFrameSizeMigration.name())
                            .about("Caches size for existing data frames")
                            .arg(
                                Arg::new("PATH")
                                    .help("Directory in which to apply the migration")
                                    .required(true),
                            )
                            .arg(
                                Arg::new("all")
                                    .long("all")
                                    .short('a')
                                    .help(
                                        "Run the migration for all oxen repositories in this directory",
                                    )
                                    .action(clap::ArgAction::SetTrue),
                            ),
                    )
                    .subcommand(
                        Command::new(PropagateSchemasMigration.name())
                            .about("Propagates schemas to the latest commit")
                            .arg(
                                Arg::new("PATH")
                                    .help("Directory in which to apply the migration")
                                    .required(true),
                            )
                            .arg(
                                Arg::new("all")
                                    .long("all")
                                    .short('a')
                                    .help(
                                        "Run the migration for all oxen repositories in this directory",
                                    )
                                    .action(clap::ArgAction::SetTrue),
                            ),
                    )
                    .subcommand(
                        Command::new(UpdateVersionFilesMigration.name())
                            .about("Migrates version files from commit id to common prefix")
                            .arg(
                                Arg::new("PATH")
                                    .help("Directory in which to apply the migration")
                                    .required(true),
                            )
                            .arg(
                                Arg::new("all")
                                    .long("all")
                                    .short('a')
                                    .help(
                                        "Run the migration for all oxen repositories in this directory",
                                    )
                                    .action(clap::ArgAction::SetTrue),
                            ),
                    )
                    .subcommand(
                        Command::new(CreateMerkleTreesMigration.name())
                        .about("Reformats the underlying data model into merkle trees for storage and lookup efficiency")
                        .arg(
                            Arg::new("PATH")
                                .help("Directory in which to apply the migration")
                                .required(true),
                        )
                        .arg(
                            Arg::new("all")
                                .long("all")
                                .short('a')
                                .help(
                                    "Run the migration for all oxen repositories in this directory",
                                )
                                .action(clap::ArgAction::SetTrue),
                        ),
                    )
                    .subcommand(
                        Command::new(AddDirectoriesToCacheMigration.name())
                        .about("SERVER ONLY: Re-caches past commits to include directories in the cache")
                        .arg(
                            Arg::new("PATH")
                                .help("Directory in which to apply the migration")
                                .required(true),
                        )
                        .arg(
                            Arg::new("all")
                                .long("all")
                                .short('a')
                                .help(
                                    "Run the migration for all oxen repositories in this directory",
                                )
                                .action(clap::ArgAction::SetTrue),
                        ),
                    )
                )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        if let Some((direction, args)) = args.subcommand() {
            match direction {
                "up" | "down" => {
                    if let Some((migration, args)) = args.subcommand() {
                        if migration == UpdateVersionFilesMigration.name() {
                            MigrateCmd::run_migration(
                                &UpdateVersionFilesMigration,
                                direction,
                                args,
                            )?
                        } else if migration == PropagateSchemasMigration.name() {
                            MigrateCmd::run_migration(
                                &PropagateSchemasMigration,
                                direction,
                                args,
                            )?
                        } else if migration == CacheDataFrameSizeMigration.name() {
                            MigrateCmd::run_migration(
                                &CacheDataFrameSizeMigration,
                                direction,
                                args,
                            )?
                        } else if migration == CreateMerkleTreesMigration.name() {
                            MigrateCmd::run_migration(
                                &CreateMerkleTreesMigration,
                                direction,
                                args,
                            )?
                        } else if migration == AddDirectoriesToCacheMigration.name() {
                            MigrateCmd::run_migration(
                                &AddDirectoriesToCacheMigration,
                                direction,
                                args,
                            )?
                        } else {
                            return Err(OxenError::basic_str(format!(
                                "Invalid migration: {}",
                                migration
                            ))); // Adjust this line for your error type.
                        }
                    }
                }
                command => {
                    return Err(OxenError::basic_str(format!(
                        "Invalid subcommand: {}",
                        command
                    ))); // Adjust this line for your error type.
                }
            }
        }
        Ok(())
    }
}

impl MigrateCmd {
    pub fn run_migration(
        migration: &dyn Migrate,
        direction: &str,
        args: &ArgMatches,
    ) -> Result<(), OxenError> {
        let path_str = args.get_one::<String>("PATH").expect("required");
        let path = Path::new(path_str);

        let all = args.get_flag("all");

        match direction {
            "up" => {
                migration.up(path, all)?;
            }
            "down" => {
                migration.down(path, all)?;
            }
            _ => {
                eprintln!("Invalid migration direction: {}", direction);
            }
        }

        Ok(())
    }
}
