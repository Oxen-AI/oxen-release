from __future__ import annotations

import logging
import os
import tempfile
from typing import Optional

import fsspec
from fsspec.utils import infer_storage_options
from .remote_repo import RemoteRepo

logger = logging.getLogger(__name__)


class OxenFS(fsspec.AbstractFileSystem):
    """
    Oxen backend for fsspec.
    """

    def __init__(
        self,
        namespace: str,
        repo: str,
        host: str = "hub.oxen.ai",
        revision: str = "main",
        scheme: str = "https",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.repo = RemoteRepo(f"{namespace}/{repo}", host, revision, scheme)
        logger.debug(f"Initialized OxenFS for {namespace}/{repo}@{revision}")

    def ls(self, path: str = "", detail: bool = False):
        return self.repo.ls(path)

    def _open(self, path: str, mode: str = "rb", **kwargs):
        """
        Open a file in the OxenFS backend.

        This is normally called through `OxenFS.open()` or `fsspec.open()`.
        """
        if mode == "rb":
            return self._open_read(path, **kwargs)
        if mode == "wb":
            return self._open_write(path, **kwargs)
        else:
            raise ValueError(
                "Unsupported file mode. Only rb and wb modes are supported"
            )

    def _open_read(self, path: str, **kwargs):
        logger.debug(f"Opening file {path} for reading")
        metadata = self.repo.metadata(path)
        if metadata.is_dir:
            raise ValueError("Cannot open directories")
        tmp_file = tempfile.NamedTemporaryFile()
        dst_path = tmp_file.file.name
        self.repo.download(path, dst_path)
        return open(dst_path, "rb")

    def _open_write(
        self,
        path: str,
        commit_message: Optional[str] = None,
        **kwargs,
    ):
        path = os.path.normpath(path)
        logger.debug(f"Opening file {path} for writing")
        target_dir = os.path.dirname(path)
        file_name = os.path.basename(path).strip()
        if file_name == "" or file_name == ".":
            raise ValueError("File name cannot be empty")
        try:
            metadata = self.repo.metadata(target_dir)
            if not metadata.is_dir:
                raise ValueError("target_dir cannot be an existing file")
        except ValueError as e:
            if "not found" in str(e):
                # If the directory does not exist, it will be created on the server
                pass
            else:
                raise e

        return OxenFSFileWriter(self.repo, file_name, target_dir, commit_message)

    @classmethod
    def _strip_protocol(cls, path):
        opts = infer_storage_options(path)
        if "username" not in opts:
            return super()._strip_protocol(path)
        return opts["path"].lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(path):
        opts = infer_storage_options(path)
        if "username" not in opts:
            return {}
        out = {"namespace": opts["username"], "repo": opts["password"]}
        if opts["host"]:
            out["revision"] = opts["host"]
        return out


class OxenFSFileWriter:
    """
    A file writer for the OxenFS backend.
    """

    def __init__(
        self,
        repo: RemoteRepo,
        path: str,
        target_dir: str = "",
        commit_message: Optional[str] = None,
    ):
        self.repo = repo
        self.path = path
        self.commit_message = commit_message or "Auto-commit from OxenFS"
        self.target_dir = target_dir
        self._tmp_file = tempfile.NamedTemporaryFile()
        logger.debug(f"Initialized OxenFSFileWriter for {path} in {target_dir}")

    def __enter__(self) -> OxenFSFileWriter:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            logger.error(
                f"Error writing to {self.repo} {self.path}: {exc_type} {exc_value} {traceback}"
            )

        self.close()
        # Don't suppress exceptions
        return False

    def write(self, data: str | bytes):
        """
        Write string or binary data to the file.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._tmp_file.write(data)

    def flush(self):
        """
        Flush the file to disk.
        """
        self._tmp_file.flush()

    def commit(self, commit_message: Optional[str] = None):
        """
        Commit the file to the remote repo.
        """
        logger.debug(f"Committing file {self.path} to {self.target_dir}")
        self.repo.upload(
            self._tmp_file.name,
            commit_message=commit_message or self.commit_message,
            file_name=self.path,
            dst_dir=self.target_dir,
        )
        logger.info(f"Committed file {self.path} to {self.target_dir}")

    def close(self):
        """
        Close the file writer. This will commit the file to the remote repo.
        """
        logger.debug(f"Closing OxenFSFileWriter for {self.path} in {self.target_dir}")
        self.flush()
        self.commit()
        self._tmp_file.close()
        logger.debug(f"Closed OxenFSFileWriter for {self.path} in {self.target_dir}")
