from .oxen import auth, util
from typing import Optional

def config_auth(host: str, token: str, path: Optional[str] = None):
    if path is None:
        path = f"{util.get_oxen_config_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        raise ValueError("Path must end with .toml")
    auth.add_host_auth(host, token, path)
