from .oxen import auth, util
from typing import Optional

def config_auth(host: str, token: str, path: Optional[str] = None):
    """
    Configures authentication for a host.
    
    Args:
        host: `str`
            The host to configure authentication for. For example: 'hub.oxen.ai'
        token: `str`
            The token to use for authentication.
        path: `Optional[str]`
            The path to save the authentication config to.
            Defaults to $HOME/.config/oxen/user_config.toml
    """
    if path is None:
        path = f"{util.get_oxen_config_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        raise ValueError("Path must end with .toml")
    auth.add_host_auth(host, token, path)
