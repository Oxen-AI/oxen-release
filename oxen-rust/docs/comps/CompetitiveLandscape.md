# Competitors

- DVC
- git lfs
- Weights & Biases
- perforce

# Why Oxen?

- Separate data and code
    - Different datasets can be used with the same codebase
    - Share datasets without sharing code
    - Easy to accidentally commit data to the codebase
- Datasets have many files, or long csvs that Oxen works well with natively

# DVC Comparison

For DVC you put your data within a git repository. There is also an option to do without an existing version control system...TODO look into this.

```shell
$ git init .

$ dvc init

$ git commit -m "Initialize DVC"

$ dvc add img_align_celeba/
```

The `dvc add` command computes all the hashes of the data, as well as copies it all over to a directory as far as I can tell. It takes awhile....To be exact:

```shell
$ time dvc add img_align_celeba/
________________________________________________________
Executed in  354.10 secs    fish           external
   usr time  179.23 secs    0.31 millis  179.23 secs
   sys time  147.19 secs    1.68 millis  147.19 secs
```


```shell
time dvc status
Data and pipelines are up to date.                                    

________________________________________________________
Executed in    9.72 secs    fish           external
   usr time    2.68 secs    0.32 millis    2.68 secs
   sys time    5.79 secs    1.91 millis    5.79 secs
```
