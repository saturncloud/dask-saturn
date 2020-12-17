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

    def __init__(
        self,
        base_url: Optional[str] = None,
        saturn_token: Optional[str] = None,
    ):
        if base_url:
            self.BASE_URL = base_url
        else:
            try:
                self.BASE_URL = os.environ["BASE_URL"]
            except KeyError as err:
                err_msg = "Missing required value Saturn url."
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
                err_msg = "Missing required value Saturn api token."
                raise RuntimeError(err_msg) from err

    @property
    def url(self):
        """Saturn url"""
        return self.BASE_URL

    @property
    def headers(self):
        """Saturn auth headers"""
        return {"Authorization": f"token {self.SATURN_TOKEN}"}
