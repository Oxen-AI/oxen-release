# Integrate Server and CLI

The Oxen Server and CLI communicate via HTTP, and we would like the interfaces for local and remote to be straight forward and easy to use.

We will continue to work off of the example of [listing branches](AddLibraryCode.md), but this time doing it on the remote server. Within the oxen library there is an `api` module that is responsible for interacting with our data models. The code for interacting with remote branches lives in [src/lib/src/api/remote/branches.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/api/remote/branches.rs).

If you have followed the [Integrate Server Code](IntegrateServerCode.md) example, you should have an endpoint up and running that can list branches.


