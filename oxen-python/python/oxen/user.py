from .oxen import user, util
from typing import Optional

def config_user(name: str, email: str, path: Optional[str] = None):
    if path is None:
        path = f"{util.get_oxen_config_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        raise ValueError(f"Path {path} must end with .toml")
    return user.config_user(name, email, path)

def current_user(path: Optional[str] = None):
    if path is None:
        path = f"{util.get_oxen_config_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        raise ValueError(f"Path {path} must end with .toml")
    return user.current_user(path)