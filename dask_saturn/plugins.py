"""
Worker plugins to be used with SaturnCluster objects.
"""
import os
import subprocess

from distributed.comm import resolve_address
from distributed.worker import get_client


class SaturnSetup:
    """WorkerPlugin that finishes setting up SaturnCluster."""

    name = "saturn_setup"

    # pylint: disable=no-self-use
    def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        worker.scheduler.addr = resolve_address(worker.scheduler.addr)


def update_files(client, path):
    tmp_path = None
    if os.path.isdir(path):
        tmp_path = "/tmp/data.tar.gz"
        subprocess.run(f"tar  --exclude .git -cvzf {tmp_path} -C {path} .", shell=True, check=True)
    with open(tmp_path or path, "rb") as f:
        payload = f.read()
    if path in list(client.datasets):
        client.unpublish_dataset(path)
    client.publish_dataset(**{path: payload})
    client.run(_register_files, client, [path])


def _register_files(client, paths):
    for path in paths:
        if path.endswith("/"):
            isdir = True
            tmp_path = "/tmp/data.tar.gz"
        with open(tmp_path or path, "wb+") as f:
            f.write(client.get(path))
        if isdir:
            subprocess.run(f"mkdir -p {path}", shell=True, check=True)
            subprocess.run(f"tar -xvzf {tmp_path} -C {path}", shell=True, check=True)
    return os.listdir()


class UploadFile:
    """WorkerPlugin for uploading files or directories to dask workers."""

    def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        with get_client(worker) as c:
            _register_files(c, list(c.datasets))
