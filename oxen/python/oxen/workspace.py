from typing import Optional, TYPE_CHECKING

from .oxen import PyWorkspace

# Use TYPE_CHECKING for type hints to avoid runtime circular imports
if TYPE_CHECKING:
    from .remote_repo import RemoteRepo


class Workspace:
    """
    The Workspace class allows you to interact with an Oxen workspace
    without downloading the data locally.

    Workspaces can be created off a branch and is tied to the commit id of the branch
    at the time of creation.

    You can commit a Workspace back to the same branch if the branch has not
    advanced, otherwise you will have to commit to a new branch and merge.

    ## Examples

    ### Adding Files to a Workspace

    Create a workspace from a branch.

    ```python
    from oxen import RemoteRepo
    from oxen import Workspace

    # Connect to the remote repo
    repo = RemoteRepo("ox/CatDogBBox")

    # Create the workspace
    workspace = Workspace(repo, "my-branch")

    # Add a file to the workspace
    workspace.add("my-image.png")

    # Print the status of the workspace
    status = workspace.status()
    print(status.added_files())

    # Commit the workspace
    workspace.commit("Adding my image to the workspace.")
    ```
    """

    def __init__(
        self,
        repo: "RemoteRepo",
        branch: str,
        workspace_id: Optional[str] = None,
        path: Optional[str] = None,
    ):
        """
        Create a new Workspace.

        Args:
            repo: `PyRemoteRepo`
                The remote repo to create the workspace from.
            branch: `str`
                The branch name to create the workspace from. The workspace
                will be tied to the commit id of the branch at the time of creation.
            workspace_id: `Optional[str]`
                The workspace id to create the workspace from.
                If left empty, will create a unique workspace id.
        """
        self._repo = repo
        self._workspace = PyWorkspace(repo._repo, branch, workspace_id, path)
        print(f"Created workspace with id: {self._workspace.id()}")

    def __repr__(self):
        return f"Workspace({self._workspace.id()}, {self._workspace.branch()})"

    def id(self):
        return self._workspace.id()

    def branch(self):
        return self._workspace.branch()

    def status(self, path: str = ""):
        """
        Get the status of the workspace.

        Args:
            path: `str`
                The path to check the status of.
        """
        return self._workspace.status(path)

    def add(self, src: str, dst: str = ""):
        """
        Add a file to the workspace

        Args:
            src: `str`
                The path to the local file to be staged
            dst: `str`
                The path in the remote repo where the file will be added
        """
        self._workspace.add(src, dst)

    def rm(self, path: str):
        """
        Remove a file from the workspace

        Args:
            path: `str`
                The path to the file on workspace to be removed
        """
        self._workspace.rm(path)

    def commit(
        self,
        message: str,
        branch_name: Optional[str] = None,
        should_delete: bool = False,
    ):
        """
        Commit the workspace to a branch

        Args:
            message: `str`
                The message to commit with
            branch_name: `Optional[str]`
                The name of the branch to commit to. If left empty, will commit to the branch
                the workspace was created from.
            should_delete: `bool`
                Whether to delete the workspace after the commit.
        """
        if branch_name is None:
            branch_name = self._workspace.branch()
        return self._workspace.commit(message, should_delete, branch_name)
