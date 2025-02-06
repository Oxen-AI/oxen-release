import os
from oxen import RemoteRepo, Workspace


def test_commit_to_named_workspace(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed

    # Create a new workspace on the main branch
    workspace = Workspace(remote_repo, branch="main", workspace_name="my-workspace")
    # Add a file to the new workspace
    image_path_1 = os.path.join(shared_datadir, "CelebA/images/2.jpg")
    workspace.add(image_path_1)
    assert len(remote_repo.list_workspaces()) == 1
    # Commit the changes
    workspace.commit("Adding a new image to the feature branch", should_delete=True)
    assert len(remote_repo.list_workspaces()) == 1
    workspace = Workspace(remote_repo, branch="main", workspace_name="my-workspace")
    assert workspace.name() == "my-workspace"
    assert len(remote_repo.list_workspaces()) == 1
    workspace.delete()
    assert len(remote_repo.list_workspaces()) == 0


def test_named_workspace_naming_collision(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, branch="main", workspace_name="my-workspace")
    workspace = Workspace(remote_repo, branch="main", workspace_name=workspace.id())
    workspace = Workspace(remote_repo, branch="main", workspace_id=workspace.id())
    workspace = Workspace(remote_repo, branch="main", workspace_name=workspace.id())
    assert workspace.name() == "my-workspace"
    assert len(remote_repo.list_workspaces()) == 1
