# oxen push

Documenting performanance on pushing a single large file before and after parallelization.

## 500 MB

Pushing `518M` file `train.arrow` from WikiText

394.09 secs = ~ 6.5 mins

Pushing with parallelization cuts in half:

209.45 secs = 3.49 mins

## 100 MB

Pushing `116M` file

