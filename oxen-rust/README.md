# Indexer

A tool to stage, commit, and push data to our servers

## File Structure

```
.indexer/
  staged
  synced
  commits/
    2022_02_10_3214312
    2022_02_10_3214332
    2022_02_10_3214541
  
```

## staged file

Line delimited list of files we want to commit

```
/Users/gregschoeninger/data/images/img1.png
/Users/gregschoeninger/data/images/img2.jpg
/Users/gregschoeninger/data/text/1.txt
/Users/gregschoeninger/data/text/2.txt
```

## synced file

Ordered & line delimited file of which commits have been synced. 

```
2022_02_10_3214312
2022_02_10_3214332
2022_02_10_3214541
```

## Commit file

A commit is a set of files we want to add to a dataset. 
Filename is the local timestamp the commit was made. 

2022_02_10_3214312
```
424582A0F3E753A9453BFAB1A67B7F3F25392AC7546AE0FC52FBE616A89B154B
/Users/gregschoeninger/data/images/img1.png
/Users/gregschoeninger/data/images/img2.jpg
/Users/gregschoeninger/data/text/1.txt
/Users/gregschoeninger/data/text/2.txt
```