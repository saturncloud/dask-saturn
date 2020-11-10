"""
imports added so users do not have to think about submodules
"""

from .core import describe_sizes, list_sizes, SaturnCluster  # noqa: F401
from ._version import get_versions
from .plugins import SaturnSetup, RegisterFiles, sync_files  # noqa: F401

__version__ = get_versions()["version"]
del get_versions
