import os
from oxen import Repo


def rcount_files_in_dir(directory: str) -> int:
    """
    Counts the number of files in a repo recursively.
    """
    return sum([len(files) for _, _, files in os.walk(directory)])


def rcount_files_in_repo(repo: Repo) -> int:
    """
    Counts the number of files in a repo recursively ignoring the .oxen directory.
    """
    total = 0
    for root, _, files in os.walk(repo.path):
        if ".oxen" in root:
            continue
        total += len(files)
    return total
