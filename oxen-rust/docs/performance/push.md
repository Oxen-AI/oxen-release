# oxen push

Documenting performanance on pushing a single large file before and after parallelization.

## 500 MB

Pushing `518M` file `train.arrow` from WikiText

394.09 secs = ~ 6.5 mins

Pushing with parallelization cuts in half:

209.45 secs = 3.49 mins

With 8 cores: 194.40 secs = 3.2 mins
With num_cpus * 2 = 36 = 174.35 = 2.9 mins

## 100 MB

Pushing `116M` file

