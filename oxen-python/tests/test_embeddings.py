import pandas as pd
import pytest
import os
from oxen import Workspace, DataFrame


def test_workspace_df_query_embeddings(question_embeddings_remote_repo_fully_pushed):
    _, remote_repo = question_embeddings_remote_repo_fully_pushed
    workspace = Workspace(remote_repo, "main")

    remote_df = DataFrame(workspace, "question_embeddings.parquet")

    # 290 is the row for the letter "A"
    rows = remote_df.get_by({"id": "290"})
    assert rows is not None
    assert len(rows) == 1
    assert rows[0]["id"] == "290"
    assert rows[0]["title"] == "A"


def test_workspace_df_get_embedding(question_embeddings_remote_repo_fully_pushed):
    _, remote_repo = question_embeddings_remote_repo_fully_pushed
    workspace = Workspace(remote_repo, "main")

    remote_df = DataFrame(workspace, "smol.jsonl")

    embeddings = remote_df.get_embeddings({"id": "2"})
    print(embeddings)
    embedding = embeddings[0]
    assert len(embedding) == 3
    assert embedding[0] == 0.2
    assert embedding[1] == 0.3
    assert embedding[2] == 0.4


def test_workspace_df_sort_by_embedding(question_embeddings_remote_repo_fully_pushed):
    _, remote_repo = question_embeddings_remote_repo_fully_pushed
    workspace = Workspace(remote_repo, "main")

    remote_df = DataFrame(workspace, "question_embeddings.parquet")

    remote_df.index_embeddings(column="question_embeddings")

    rows = remote_df.nearest_neighbors(
        find_embedding_where={"title": "A"}, sort_by_similarity_to="question_embeddings"
    )

    assert len(rows) > 1
    assert rows[0]["id"] == "290"
    assert rows[0]["title"] == "A"
