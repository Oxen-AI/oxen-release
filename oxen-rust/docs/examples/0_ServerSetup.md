
# Develper Setup for CLI and Server

Build the Oxen Server and Oxen CLI binaries

`cargo build`

Generate a config file that contains an access token to give it to the user to access to the server

`./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output auth_config.toml`

The user who needs access should copy the config to the ~/.oxen directory, which is where the Oxen CLI looks for it. If the user has not done this step, they will not have access to the server.

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

If you want to change the default `IP ADDRESS` and `PORT` you can do so by passing them in with the `-i` and `-p` parameters.

`./target/debug/oxen-server start -i 0.0.0.0 -p 4004`