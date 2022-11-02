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

