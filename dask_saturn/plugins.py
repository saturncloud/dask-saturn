"""
Worker plugins to be used with SaturnCluster objects.
"""

from distributed.comm import resolve_address


class SaturnSetup:
    """WorkerPlugin that finishes setting up SaturnCluster."""

    name = "saturn_setup"

    # pylint: disable=no-self-use
    def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        worker.scheduler.addr = resolve_address(worker.scheduler.addr)
