import oxen
import logging

from oxen.repos import Repo, RemoteRepo
# from oxen.branches import get_branch

import oxen

FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
# logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.INFO)

# oxen.clone("https://hub.oxen.ai/nurul-oxen/Laion-100K", "/tmp/test")

# repo = RemoteRepo("ox/chatbot", host="0.0.0.0:4000", use_ssl=False)
# branch = repo.get_branch("main")
# print(f"Got branch! {branch} {branch.name} {branch.commit_id}")

# DEFINE HIGH LEVEL FUNCTIONS

# TODO: just start with all the basic functions, with minimal params, and extend
# start cleaning up/documenting Oxen rust repo in parallel, and making the command module call out to sub modules, but keep high level interface the same
# look into docs.rs for rust docs

# Local

# * init
# * clone
# * add
# * commit
# * checkout
# * create_branch
# * list_branches
# * push
# * pull

# Remote



repo = Repo("/path/to/repo")
repo = Repo.clone("https://hub.oxen.ai/nurul-oxen/Laion-100K", "/tmp/test")

repo.pull() # origin, master
repo.add("test.py")
repo.commit("My message")
repo.push() # origin, master


# Remote 
remote_repo = RemoteRepo.init("ox/chatbot", branch="main")
remote_repo.add("data.parquet")
remote_repo.commit("Initial commit")

dataset = remote_repo.file("data.parquet")
dataset = remote_repo.dir("data/")