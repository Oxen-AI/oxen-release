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


def test_workspace_df_sort_by_embedding_nearest_neighbors(
    question_embeddings_remote_repo_fully_pushed,
):
    _, remote_repo = question_embeddings_remote_repo_fully_pushed
    workspace = Workspace(remote_repo, "main")

    remote_df = DataFrame(workspace, "smol.jsonl")

    column = "embedding"
    is_indexed = remote_df.is_nearest_neighbors_enabled(column=column)
    assert is_indexed == False

    remote_df.enable_nearest_neighbors(column=column)

    embedding = [0.3, 0.4, 0.5]

    results = remote_df.sort_by_embedding(column, embedding, page_size=2)
    print(results)
    assert len(results) == 2
    assert results[0]["id"] == "3"
    assert results[0]["embedding"] == embedding


def test_workspace_df_sort_by_embedding_search(
    question_embeddings_remote_repo_fully_pushed,
):
    _, remote_repo = question_embeddings_remote_repo_fully_pushed
    workspace = Workspace(remote_repo, "main")

    remote_df = DataFrame(workspace, "question_embeddings.parquet")

    embeddings_column = "question_embeddings"

    is_indexed = remote_df.is_nearest_neighbors_enabled(column=embeddings_column)
    assert is_indexed == False

    remote_df.enable_nearest_neighbors(column=embeddings_column)

    rows = remote_df.nearest_neighbors_search(
        find_embedding_where={"title": "A"}, sort_by_similarity_to=embeddings_column
    )

    assert len(rows) > 1
    assert rows[0]["id"] == "290"
    assert rows[0]["title"] == "A"

    is_indexed = remote_df.is_nearest_neighbors_enabled(column=embeddings_column)
    assert is_indexed == True
