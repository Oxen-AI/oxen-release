from oxen.loaders import ChatLoader
import os


def test_chat_loader(chat_bot_local_repo_no_commits):
    repo = chat_bot_local_repo_no_commits
    prompt_file = os.path.join(repo.path, "prompt.txt")
    data_file = os.path.join(repo.path, "examples.tsv")
    loader = ChatLoader(prompt_file, data_file)
    result = loader.run()[0]
    assert result.height == 6
