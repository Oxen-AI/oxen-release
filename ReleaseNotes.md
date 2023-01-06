# v0.4.1

* Features
    * Faster data download 🔥
    * Chunked download data APIs

# v0.4.0

* Features
    * Faster data upload 🔥
    * Chunked upload data APIs

* Breaking Changes
    * Removed CADF
    * Remove `oxen index` commands

# v0.3.0

* Features
    * `oxen df --unique`
    * `oxen rm`
* Schema changes
    * add "email" to Commit
    * changed "timestamp" format on Commit to be RFC3339

# v0.2.7

* Features
    * oxen-server
        * HTTP APIs for fetching schemas and data frames
        * return more robust metadata on listing directories
    * oxen
        * support for multi argument filters ie "label == person && is_correct == true"
        * support for single or double quotes on aggregations
* Bug Fixes
    * fix restore tabular modified file

# v0.2.6

* Features
    * `oxen add` accepts multiple values
* Bug fixes
    * `oxen checkout` arg parsing
    * `oxen commit` files without extensions

# v0.2.5

* Bug fixes
    * Better logging
    * Don't fail silently on remote push

# v0.2.4

* Bug fixes
    * Handle duplicate lines in tabular data files
    * Fix `/dir` endpoint when no path is specified
    * Re-enable `/file` endpoint
* Add more tests to catch tabular data edge cases

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

