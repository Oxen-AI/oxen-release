# Read up on data, dataset, and dataloader APIs in
#   * pytorch
#   * huggingface
#   * tensorflow
#
# Others to consider:
#   * keras
#   * fastai
#   * pytorch-lightning
#   * jax
#   * sklearn
#   * pandas
#   * numpy
#   * scipy

# Start going through tutorials for each of the above,
# and write example code for each of the above
# fit into a boilerplate similar to the ImageClassificationBoilerplate and
# be sure that the Oxen data APIs can easily slot in,
# with swapping out the model, training loop, eval loop, etc.

# Design APIs to be easy to fetch random examples from remote data frames to serve up
# to our "gradio chatbot labeling tool" (or any other labeling tool)

# Design underlying Oxen APIs to easily conform to the above APIs
#
# Keith: How impossible would building a Python library that exposes a read-only
# dataframe on top of an oxen repo be?

# Read-only dataframe API:
# 0) download a file to disk from repo (easy)
# 1) load a dataframe into python, from downloaded file (easy)
# 2) slice and stream a dataframe in current format (easy, already have apis,
#    potentially slow to slice big CSVs)
# 3) slice and stream a dataframe after converting for apache arrow for you on disk
#    (way more efficient, can convert on disk, do we do out of the box?)

from oxen import PyDataset

import os
import polars as pl
from typing import Sequence, Union
from pathlib import Path
from typing import Optional


class Dataset:
    """
    Dataset object constructs a dataset from a remote or local repo.
    It can be used to load data into a dataloader.
    """

    # TODO: allow remote or local repos
    def __init__(
        self,
        repo,
        paths: Union[str, Sequence[str]],
        features=None,
        cache_dir: str = None,
        download: bool = False,
    ):
        """
        Create a new RemoteRepo object to interact with.

        Parameters
        ----------
        repo : Repo
            The oxen repository you are loading data from
            can be a local or a remote repo
        features : Features | None
            The features of the dataset, columns, dtypes, etc.
        paths : str | Sequence[str]
            The paths to the data files needed to load the dataset
        cache_dir : str
            The directory to download/cache the data in.
        download : bool
            Whether to download the data or not.
        """
        self._repo = repo
        if cache_dir is None:
            self._cache_dir = Dataset.default_cache_dir(repo)
        else:
            self._cache_dir = cache_dir

        if isinstance(paths, str):
            self._paths = [paths]
        else:
            self._paths = paths

        self._data_frames = []
        self._features = features

        if download:
            self.download_all()

    # For iterating over the dataset
    def __len__(self):
        return sum([df.height for df in self._data_frames])

    # For iterating over the dataset
    def __getitem__(self, idx):
        # iterate over data frames to find the right one to index
        df_offset = 0  # which row we are at
        df_idx = 0  # which dataframe we are on
        while df_offset < idx:
            df_offset += self._data_frames[df_idx].height
            df_idx += 1

        # Offset is the row we are at in the data frame
        remainder = idx - df_offset

        # extract the features from the data frame
        ret_features = []
        for feature in self.features:
            df = self._data_frames[df_idx]
            val = df[feature.name][remainder]
            ret_features.append(val)

        return ret_features

    @staticmethod
    def default_cache_dir(repo) -> str:
        # ~/.oxen/.cache/<namespace>/<repo>/<revision>
        cache_dir = os.path.join(Path.home(), ".oxen")
        cache_dir = os.path.join(cache_dir, ".cache")
        cache_dir = os.path.join(cache_dir, repo.namespace)
        cache_dir = os.path.join(cache_dir, repo.name)
        cache_dir = os.path.join(cache_dir, repo.revision)
        return cache_dir

    def _cache_download_path(self, base_dir: Optional[str]) -> str:
        """
        Returns the path to the file given the base dir or defaults to
        the cache directory.
        """
        if base_dir is None:
            return self._cache_dir
        return base_dir

    def df(self, path: str, base_dir: str = None) -> pl.DataFrame:
        """
        Returns a dataframe of the data from the repo, fully loaded into memory.

        Parameters
        ----------
        path : str
            Paths to the data frames you want to load.
        base_dir : str | None
            The base directory to download the data to.
        """
        # TODO:
        #  * handle streaming into memory without downloading to disk

        output_path = self.download(path, base_dir)
        df = PyDataset.df(output_path)
        return df

    def download(self, remote_path: str, base_dir: str = None) -> str:
        """
        Returns the path to the downloaded file.

        Parameters
        ----------
        path : str
            Paths to the file you want to load.
        base_dir : str | None
            The base directory to download the data to.
        """
        # Download file to cache path if it does not already exist
        # Cache path has the revision, so if the revision changes, it will
        # download the new data
        base_dir = self._cache_download_path(base_dir)
        output_path = os.path.join(base_dir, remote_path)
        parent = os.path.dirname(output_path)
        if not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        if not os.path.exists(output_path):
            self._repo.download(remote_path, parent)
        return output_path

    def download_all(self):
        """
        Download the data from the repo to disk
        """
        for path in self._paths:
            print(f"Downloading {path}...")

            path = self.download(path, self._cache_dir)
