import os

from oxen import diff_tabular


def test_diff_added_row(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "Diffs")

    diff = diff_tabular(
        left=os.path.join(repo_dir, "prompts.csv"),
        right=os.path.join(repo_dir, "prompts_added_row.csv"),
        keys=[],  # No keys
        targets=[],  # No targets
    )

    print(diff)

    assert diff.shape[0] == 1
    assert diff.shape[1] == 3
