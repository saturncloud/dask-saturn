"""
Settings used for interacting with Saturn
"""

import os

from typing import Optional


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
        if base_url:
            self.BASE_URL = base_url
        else:
            try:
                self.BASE_URL = os.environ["BASE_URL"]
            except KeyError as err:
                if is_external:
                    err_msg = "Missing required value BASE_URL."
                else:
                    err_msg = "Required environment variable BASE_URL not set."
                raise RuntimeError(err_msg) from err

        if saturn_token:
            self.SATURN_TOKEN = saturn_token
        else:
            try:
                self.SATURN_TOKEN = os.environ["SATURN_TOKEN"]
            except KeyError as err:
                if is_external:
                    err_msg = "Missing required value SATURN_TOKEN."
                else:
                    err_msg = "Required environment variable SATURN_TOKEN not set."
                raise RuntimeError(err_msg) from err

    def _get_headers(self, token: Optional[str] = None):
        """Return Saturn auth headers"""
        return {"Authorization": f"token {token or self.SATURN_TOKEN}"}
