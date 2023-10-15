from .oxen import auth, util
from typing import Optional


def config_auth(token: str, host: str = "hub.oxen.ai", path: Optional[str] = None):
    """
    Configures authentication for a host.

    Args:
        token: `str`
            The token to use for authentication.
        host: `str`
            The host to configure authentication for. Default: 'hub.oxen.ai'
        path: `Optional[str]`
            The path to save the authentication config to.
            Defaults to $HOME/.config/oxen/user_config.toml
    """
    if path is None:
        path = f"{util.get_oxen_config_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        raise ValueError("Path must end with .toml")
    auth.add_host_auth(host, token, path)
