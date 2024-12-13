import pandas as pd
import pytest
import os
from oxen import Workspace, DataFrame


def test_workspace_df_sql_query(
    question_embeddings_remote_repo_fully_pushed, shared_datadir
):
    _, remote_repo = question_embeddings_remote_repo_fully_pushed
    workspace = Workspace(remote_repo, "main")

    remote_df = DataFrame(workspace, "question_embeddings.parquet")

    sql = remote_df.select_sql_from_dict({"id": "290"})
    print(sql)
    assert sql == "SELECT * FROM df WHERE id = '290'"

    sql = remote_df.select_sql_from_dict(
        {"id": "290", "question": "What is the capital of France?"}
    )
    print(sql)
    assert (
        sql
        == "SELECT * FROM df WHERE id = '290' AND question = 'What is the capital of France?'"
    )
