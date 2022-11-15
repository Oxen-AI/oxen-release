# v0.2.3

*  `oxen df`
    * `-a` for aggregations
    * `--sort` to sort by a field
    * `--reverse` to reverse the order
* `oxen schemas`
    * `create_index` subcommand to create an index on a field
    * `query` subcommand to query an index
* Bug fixes on indexing DataFrames

# v0.2.2

Update `oxen restore`
* `oxen restore --staged <pathspec>` will remove a file or directory from staging
* `oxen restore <pathspec>` now works recursively

# v0.2.1

Performance Improvements:
* `oxen add` opening less db connections
* `oxen push` faster local delta computation

Better Formatting
* Printing Schemas

Bug Fixes:
* `oxen pull` progress bar fixes, unpacking fixes
* Extra print statements

# v0.2.0

* Add row level annotation tracking so that you can efficiently compare tabular data
* Add `oxen df` commands to help work with tabular data
* Update underlying repository structure to be more efficient per directory and more efficient with tabular data
* Solidify underlying API routes for HTTP server
* Add optional authorization on Oxen Server
* Can delete remote branches

