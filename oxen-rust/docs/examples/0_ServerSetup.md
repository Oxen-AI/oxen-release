
# Setup CLI and Server

Build the binaries

`cargo build`

Generate a config file and token to give user access to the server

`./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output auth_config.toml`

Copy the config to the default locations

`mkdir ~/.oxen`

`mv auth_config.toml ~/.oxen/auth_config.toml`

`cp ~/.oxen/auth_config.toml data/test/config/auth_config.toml`

Run the server

`./target/debug/oxen-server start`

The default sync directory is `/tmp/oxen_sync` to change it set the SYNC_DIR environment variable to a path.

In fish shell an example would be

`env SYNC_DIR=/Users/gregschoeninger/Data/oxen_server ./target/debug/oxen-server start`

In bash shell

`export SYNC_DIR=/Users/gregschoeninger/Data/oxen_server ./target/debug/oxen-server start`

```
Running 🐂 server on 0.0.0.0:3000
Syncing to directory: /Users/gregschoeninger/Data/oxen_server
[2022-06-08T10:00:48Z INFO  actix_server::builder] Starting 8 workers
[2022-06-08T10:00:48Z INFO  actix_server::server] Actix runtime found; starting in Actix runtime
```