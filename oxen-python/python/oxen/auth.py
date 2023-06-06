from .oxen import auth


def create_user_config(name: str, email: str, path: str | None = None):
    if path is None:
        path = f"{auth.get_oxen_home_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        path = f"{path}/user_config.toml"
    auth.create_user_config(name, email, path)


def add_host_auth(name: str, email: str, path: str | None = None):
    if path is None:
        path = f"{auth.get_oxen_home_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        path = f"{path}/user_config.toml"
    auth.add_host_auth(name, email, path)
