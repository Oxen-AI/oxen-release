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
import polars as pl

from typing import Sequence, Union


class Dataset:
    """
    Dataset object constructs a dataset from a remote or local repo.
    It can be used to load data into a dataloader.
    """

    # TODO: allow remote or local repos
    def __init__(self, repo):
        """
        Create a new RemoteRepo object to interact with.

        Parameters
        ----------
        repo : Repo
            The oxen repository you are loading data from
            can be a local or a remote repo
        """
        self._repo = repo
        self._data_files = []

    # TODO: optionally download data
    def df(
        self,
        path: str,
    ) -> pl.DataFrame:
        """
        Returns a dataframe of the data from the repo.
            Parameters
        ----------
        path : str
            Paths to the data frames you want to load.
        """
        # TODO:
        #  * handle download = False, stream in memory
        #  * pass in destination if download = True

        self._repo.download(path, path)
        df = PyDataset.df(path)
        return df

    def load(
        self, data: Union[str, Sequence[str]], download: bool = True
    ) -> pl.DataFrame:
        """
        Load the data from the repo into a dataframe.

        Parameters
        ----------
        data : str | Sequence[str]
            Path or paths to the data frames you want to load.

            If you pass a string, it will pull the data from the  at that path.
            If you pass a sequence, it will
        """
        if isinstance(data, str):
            self._data_files = [data]
        else:
            self._data_files = data

        if download:
            for data_file in self._data_files:
                print(f"Downloading {data_file}...")
                self._repo.download(data_file, data_file)
