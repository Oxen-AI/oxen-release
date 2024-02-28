import os

from oxen import diff_tabular


def test_add(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "Diffs")

    diff = diff_tabular(
        left=os.path.join(repo_dir, "prompts_v1.csv"),
        right=os.path.join(repo_dir, "prompts_v2.csv"),
        keys=[],  # No keys
        targets=[],  # No targets
    )

    print(diff)

    assert diff.shape[0] == 1
    assert diff.shape[1] == 3
