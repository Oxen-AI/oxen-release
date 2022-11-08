# Oxen Restore

The `oxen restore` command can be useful for reverting changes in your working directory to the HEAD commit, or some specific point in the history.

For example if you modified or deleted a file that you did not intend to, simply run

```bash
$ oxen restore path/to/file.txt
```

It also works recursively to restore all changes within a directory

```bash
$ oxen restore path/to/dir
```

## Restore to version

If you want to restore to a specific version in your commit history, you can supply the commit id or branch name with the `--source` flag.

```bash
$ oxen restore path/to/file.txt --source COMMIT_ID
```

## Unstage a file

If you accidentally staged a file or directory with the `oxen add` command, and want to unstage it, you can also use the `restore` command for this.

```bash
$ oxen restore --staged path/to/dir
```
