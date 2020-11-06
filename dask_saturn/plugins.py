"""
Worker plugins to be used with SaturnCluster objects.
"""
import os
import subprocess

from distributed.comm import resolve_address
from distributed.worker import get_client, get_worker
from distributed import worker_client


class SaturnSetup:
    """WorkerPlugin that finishes setting up SaturnCluster."""

    name = "saturn_setup"

    # pylint: disable=no-self-use
    def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        worker.scheduler.addr = resolve_address(worker.scheduler.addr)


def update_files(client, path):
    tmp_path = None
    path = os.path.abspath(path)
    if os.path.isdir(path):
        path += '/'
        tmp_path = "/tmp/data.tar.gz"
        subprocess.run(f"tar --exclude .git -cvzf {tmp_path} -C {path} .", shell=True, check=True)
    with open(tmp_path or path, "rb") as f:
        payload = f.read()
    if path in list(client.datasets):
        client.unpublish_dataset(path)
    client.publish_dataset(**{path: payload})
    client.run(register_files, [path])


def register_files(paths=None):
    """Register all files in the given paths on the workers"""
    with worker_client(separate_thread=False) as client:
        if paths is None:
            paths = client.list_datasets()
        for path in paths:
            tmp_path = None
            if path.endswith("/"):
                isdir = True
                tmp_path = "/tmp/data.tar.gz"
            with open(tmp_path or path, "wb+") as f:
                f.write(client.get_dataset(path))
            if isdir:
                subprocess.run(f"mkdir -p {path}", shell=True, check=True)
                subprocess.run(f"tar -xvzf {tmp_path} -C {path}", shell=True, check=True)
    return os.listdir()


class UploadFiles:
    """WorkerPlugin for uploading files or directories to dask workers."""

    def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        register_files()
