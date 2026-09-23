"""Per-entry transport; never change global or EU proxy settings."""
from urllib.parse import urlsplit

import requests


def validate_proxy(value: str | None) -> str:
    if value is None or value == "":
        return ""
    try:
        if not isinstance(value, str) or any(c.isspace() for c in value):
            raise ValueError
        parsed = urlsplit(value)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
            raise ValueError
        _ = parsed.port
    except (ValueError, TypeError):
        raise ValueError("Invalid proxy URL") from None
    return value


def create_session(proxy_url: str = "") -> requests.Session:
    proxy_url = validate_proxy(proxy_url)
    session = requests.Session()
    session.trust_env = False
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}
    return session
