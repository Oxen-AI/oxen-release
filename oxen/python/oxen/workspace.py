
from typing import Optional
from .oxen import PyRemoteRepo, PyWorkspace

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
        repo: PyRemoteRepo,
        branch: str,
        workspace_id: Optional[str] = None,
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
        self._branch = branch
        self._workspace = PyWorkspace(repo, branch, workspace_id)

    def __repr__(self):
        return f"Workspace({self._repo.url()}, {self._branch})"

    def status(self, path: str):
        """
        Get the status of the workspace.

        Args:
            path: `str`
                The path to check the status of.
        """
        self.workspace.status(path)
        
    def add(self, src: str, dst: str = ""):
        """
        Add a file to the workspace

        Args:
            src: `str`
                The path to the local file to be staged
            dst: `str`
                The path in the remote repo where the file will be added
        """
        self._repo.add(src, dst)

    def rm(self, path: str):
        """
        Remove a file from the workspace

        Args:
            path: `str`
                The path to the file on workspace to be removed
        """
        self._repo.remove(path)

