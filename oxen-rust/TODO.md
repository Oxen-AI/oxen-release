# Merkle Tree Refactor

* File Chunk Deduplication
  * File Chunks
  * Can get rid of FileChunkNode type since it's just a file we store on disk now
  * Can optionally unpack to full file on specific commits
    * Always use chunks
    * This helps us write to the merkle tree in parallel on upload
    * If !chunking turned on in branch, we unpack chunks into full files or write to duckdb or s3 or w/e


* Make commit object another node in the merkle tree
  * It can just be a different object type
  * Commit
    * id (is just hash)
    * Author
    * Email
    * Parents
    * Timestamp
    * Root (directory node)
* Create a new ref dbs
  * can ignore the rocksdb in the base dir...
  * create new dbs for
  * refs/branches
  * refs/tags

* Storage Backends
  * Local Backend
  * S3 Backend