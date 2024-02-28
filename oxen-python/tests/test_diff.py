import os

from oxen import diff


def test_tabular_diff_added_row(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "Diffs")

    result = diff.compare(
        os.path.join(repo_dir, "prompts.csv"),
        os.path.join(repo_dir, "prompts_added_row.csv"),
    )

    print(result)

    df = result.contents
    assert df.shape[0] == 1
    assert df.shape[1] == 3

def test_text_diff_added_row(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "Diffs")

    result = diff.compare(
        os.path.join(repo_dir, "prompts.txt"),
        os.path.join(repo_dir, "prompts_added_row.txt"),
    )

    print(result)

    text_diff = result.contents
    assert len(text_diff.lines) == 4
