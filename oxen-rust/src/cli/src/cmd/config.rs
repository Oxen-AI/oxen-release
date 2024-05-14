use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::command;
use liboxen::config::{AuthConfig, UserConfig};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "config";
pub struct ConfigCmd;

#[async_trait]
impl RunCmd for ConfigCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Sets the user configuration in ~/.oxen/user_config.toml")
            .arg(
                Arg::new("name")
                    .long("name")
                    .short('n')
                    .help("Set the name you want your commits to be saved as.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("email")
                    .long("email")
                    .short('e')
                    .help("Set the email you want your commits to be saved as.")
                    .action(clap::ArgAction::Set),
            )
            // Note: we differ from git here because we have the concept of a remote
            //       staging area which uses the `oxen remote add` subcommand
            .arg(
                Arg::new("set-remote")
                    .long("set-remote")
                    .number_of_values(2)
                    .value_names(["NAME", "URL"])
                    .help("Set a remote for your current working repository.")
                    .action(clap::ArgAction::Set),
            )
            // "delete-remote" is easier to read than "remove-remote"
            .arg(
                Arg::new("delete-remote")
                    .long("delete-remote")
                    .number_of_values(2)
                    .help("Delete a remote from the current working repository.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("auth-token")
                    .long("auth")
                    .short('a')
                    .number_of_values(2)
                    .value_names(["HOST", "TOKEN"])
                    .help("Set the authentication token for a specific oxen-server host.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("default-host")
                    .long("default-host")
                    .help("Sets the default host used to check version numbers. If empty, the CLI will not do a version check.")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let mut repo = LocalRepository::from_current_dir()?;

        // Parse Args
        if let Some(remote) = args.get_many::<String>("set-remote") {
            if let [name, url] = remote.collect::<Vec<_>>()[..] {
                match self.set_remote(&mut repo, name, url) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            } else {
                eprintln!("invalid arguments for --set-remote");
            }
        }

        if let Some(name) = args.get_one::<String>("delete-remote") {
            match self.delete_remote(&mut repo, name) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        }

        if let Some(auth) = args.get_many::<String>("auth-token") {
            if let [host, token] = auth.collect::<Vec<_>>()[..] {
                match self.set_auth_token(host, token) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            } else {
                eprintln!("invalid arguments for --auth");
            }
        }

        if let Some(name) = args.get_one::<String>("name") {
            match self.set_user_name(name) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        }

        if let Some(email) = args.get_one::<String>("email") {
            match self.set_user_email(email) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        }

        if let Some(email) = args.get_one::<String>("default-host") {
            match self.set_default_host(email) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        }

        Ok(())
    }
}

impl ConfigCmd {
    pub fn set_remote(
        &self,
        repo: &mut LocalRepository,
        name: &str,
        url: &str,
    ) -> Result<(), OxenError> {
        command::config::set_remote(repo, name, url)?;

        Ok(())
    }

    pub fn delete_remote(&self, repo: &mut LocalRepository, name: &str) -> Result<(), OxenError> {
        command::config::delete_remote(repo, name)?;

        Ok(())
    }

    pub fn set_auth_token(&self, host: &str, token: &str) -> Result<(), OxenError> {
        let mut config = AuthConfig::get_or_create()?;
        config.add_host_auth_token(host, token);
        config.save_default()?;
        println!("Authentication token set for host: {host}");
        Ok(())
    }

    pub fn set_default_host(&self, host: &str) -> Result<(), OxenError> {
        let mut config = AuthConfig::get_or_create()?;
        if host.is_empty() {
            config.default_host = None;
        } else {
            config.default_host = Some(String::from(host));
        }
        config.save_default()?;
        Ok(())
    }

    pub fn set_user_name(&self, name: &str) -> Result<(), OxenError> {
        let mut config = UserConfig::get_or_create()?;
        config.name = String::from(name);
        config.save_default()?;
        Ok(())
    }

    pub fn set_user_email(&self, email: &str) -> Result<(), OxenError> {
        let mut config = UserConfig::get_or_create()?;
        config.email = String::from(email);
        config.save_default()?;
        Ok(())
    }
}
