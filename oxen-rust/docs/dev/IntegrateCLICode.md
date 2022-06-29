# How to add a command to CLI

Once you have written the library code from [this example](AddLibraryCode.md) it's time to integrate it into the CLI. The entry point for parsing command line arguments is in [src/cli/src/main.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/cli/src/main.rs). 

We use the [clap](https://docs.rs/clap/latest/clap/) library to parse the arguments. Then pass the required parameters to our [dispatch](https://github.com/Oxen-AI/Oxen/blob/main/src/cli/src/dispatch.rs) module to instantiate the repository and call the [command api](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/command.rs). 



