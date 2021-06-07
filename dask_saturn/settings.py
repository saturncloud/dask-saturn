"""
Settings used for interacting with Saturn
"""

import os

from urllib.parse import urlparse
from ._version import get_versions

__version__ = get_versions()["version"]


class Settings:
    """Global settings"""

    SATURN_TOKEN: str
    SATURN_BASE_URL: str

    def __init__(self):
        try:
            self.SATURN_BASE_URL = os.environ["SATURN_BASE_URL"]
        except KeyError as err:
            if os.environ.get("BASE_URL") is not None:
                # if ``BASE_URL`` is set and ``SATURN_BASE_URL`` isn't, it's an old
                # version of Saturn that is incompatible with this version of dask_saturn.
                err_msg = (
                    "This version of dask-saturn is incompatible with your Saturn version. "
                    "Downgrade dask-saturn to `0.2.3`: `pip install dask_saturn==0.2.3`"
                )
                raise RuntimeError(err_msg) from err

            err_msg = "Missing required environment variable SATURN_BASE_URL."
            raise RuntimeError(err_msg) from err

        parsed = urlparse(self.SATURN_BASE_URL)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f'"{self.SATURN_BASE_URL}" is not a valid URL')

        try:
            self.SATURN_TOKEN = os.environ["SATURN_TOKEN"]
        except KeyError as err:
            err_msg = "Missing required environment variable SATURN_TOKEN."
            raise RuntimeError(err_msg) from err

    @property
    def is_external(self) -> bool:
        """Whether the client environment is external to Saturn"""
        return os.environ.get("SATURN_IS_INTERNAL", "false").lower() == "false"

    @property
    def url(self):
        """Saturn url"""
        return self.SATURN_BASE_URL

    @property
    def headers(self):
        """Saturn auth headers"""
        return {
            "Authorization": f"token {self.SATURN_TOKEN}",
            "X-Dask-Saturn-Version": __version__,
        }
