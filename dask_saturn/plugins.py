"""
Worker plugins to be used with SaturnCluster objects.
"""
import os
import subprocess
from distributed.comm import resolve_address


class SaturnSetup:
    """WorkerPlugin that finishes setting up SaturnCluster."""

    name = "saturn_setup"

    # pylint: disable=no-self-use
    def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        worker.scheduler.addr = resolve_address(worker.scheduler.addr)


class GitPull:
    """WorkerPlugin that pulls from git."""

    name = "git_pull"

    def setup(self, worker=None):
        subprocess.Popen(["git", "pull"])
        return os.listdir()


class RunScript:
    """WorkerPlugin that runs the given script."""

    name = "run_script"

    def __init__(self, filename=None):
        self.filename = filename

    def setup(self, worker=None):
        subprocess.Popen(["bash", self.filename])
