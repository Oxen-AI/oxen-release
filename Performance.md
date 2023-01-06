## Initial Performance Numbers

Using the CelebA dataset as an example using Oxen

`oxen add images` takes ~10 sec
`oxen commit -m "adding images"` takes ~41 sec

Compare this to a system like [git lfs](https://git-lfs.github.com/) on the same dataset

`git lfs track images` takes ~17 sec
`git add images` takes ~136 sec
`git commit -m "adding images"` takes ~44 sec
`git remote add origin https://huggingface.co/datasets/gschoeni/CelebA`
`git push origin master`

If you add this up oxen takes ~49 sec to git's ~197 sec which is about a 4x speed improvement for adding and committing.

## How it Works

One of the first optimizations we implemented at Oxen was swapping out the hashing algorithm. The traditional SHA-1 algorithm that git uses has bandwidth about 0.8 GB/sec. This becomes a pain when dealing with larger datasets. At Oxen we chose a non-cryptographic hash function that enables speed ups to ~30 GB/sec. We also take advantage of all the cores of your machine when indexing your changes.

