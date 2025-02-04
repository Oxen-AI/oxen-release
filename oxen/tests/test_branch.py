import os


def test_branch_exists(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    # oxen add
    image_file = "images/1.jpg"
    full_path = os.path.join(repo.path, image_file)
    repo.add(full_path)

    # oxen commit
    repo.commit("Add first image")

    # oxen branch_exists
    assert repo.branch_exists("main") == True
    assert repo.branch_exists("false") == False


