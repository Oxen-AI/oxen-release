# Why Oxen?

Why build a new version control system for data?

* Speed, these datasets are large
    * Fast Hashing
    * Native Parallelization
    * Fast access kv dbs
    * Indexed rows into content addressable data frames
    * Data streaming for distributed training
* Explore your data
    * Data != code, browsing line by line is not sufficient
    * To debug you need native ability to slice, dice, cluster, within a version
* Better than zip files and checkpoints
    * Can do EDA and run experiments without downloading everything