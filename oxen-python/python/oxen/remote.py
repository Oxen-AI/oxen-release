from .oxen import remote

def get_repo(name: str, host: str = "hub.oxen.ai"):
    return remote.get_repo(name, host)

def create_repo(
    name: str,
    description="",
    is_public: bool = True,
    host: str = "hub.oxen.ai",
    files=[]
):
    return remote.create_repo(name, description, is_public, host, files)
