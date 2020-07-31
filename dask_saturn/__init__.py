from .core import SaturnCluster
from .core import describe_sizes, list_sizes

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
