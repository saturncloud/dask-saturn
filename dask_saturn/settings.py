"""
Settings used for interacting with Saturn
"""

import os

from typing import Optional
from urllib.parse import urlparse


class Settings:
    """Global settings"""

    SATURN_TOKEN: str
    BASE_URL: str
    is_external: bool

    def __init__(
        self,
        base_url: Optional[str] = None,
        saturn_token: Optional[str] = None,
        is_external: bool = False,
    ):
        self.is_external = is_external
        if base_url:
            self.BASE_URL = base_url
        else:
            try:
                self.BASE_URL = os.environ["BASE_URL"]
            except KeyError as err:
                if self.is_external:
                    err_msg = "Missing required value base_url."
                else:
                    err_msg = "Required environment variable BASE_URL not set."
                raise RuntimeError(err_msg) from err

        parsed = urlparse(self.BASE_URL)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f'"{self.BASE_URL}" is not a valid URL')

        if saturn_token:
            self.SATURN_TOKEN = saturn_token
        else:
            try:
                self.SATURN_TOKEN = os.environ["SATURN_TOKEN"]
            except KeyError as err:
                if self.is_external:
                    err_msg = "Missing required value saturn_token."
                else:
                    err_msg = "Required environment variable SATURN_TOKEN not set."
                raise RuntimeError(err_msg) from err

    def _get_headers(self, token: Optional[str] = None):
        """Return Saturn auth headers"""
        return {"Authorization": f"token {token or self.SATURN_TOKEN}"}
