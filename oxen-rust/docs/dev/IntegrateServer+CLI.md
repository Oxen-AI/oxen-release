# Integrate Server and CLI

The Oxen Server and CLI communicate via HTTP, and we would like the interfaces for local and remote to be straight forward and easy to use.

We will continue to work off of the example of listing branches, but this time doing it on the remote server. Within the oxen library there is an `api` module with different submodules for each of the data models we would want to interact with. The code for interacting with remote branches lives in [src/lib/src/api/remote/branches.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/api/remote/branches.rs).

