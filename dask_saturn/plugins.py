"""
Worker plugins to be used with SaturnCluster objects.
"""
import os
import subprocess
from typing import List, Optional

from distributed import Client
from distributed.comm import resolve_address
from distributed.worker import get_client


PREFIX = "SATURN__"


class SaturnSetup:
    """WorkerPlugin that finishes setting up SaturnCluster."""

    name = "saturn_setup"

    # pylint: disable=no-self-use
    def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        worker.scheduler.addr = resolve_address(worker.scheduler.addr)


async def register_files_to_worker(paths: Optional[List[str]] = None) -> List[str]:
    """Register all files in the given paths on the current worker"""
    with get_client() as client:
        # If paths isn't provided, register all files in datasets that start with prefix
        if paths is None:
            datasets = await client.list_datasets()
            paths = [p[len(PREFIX) :] for p in datasets if p.startswith(PREFIX)]

        for path in paths:
            # retrieve the filedata from the scheduler
            payload = await client.get_dataset(f"{PREFIX}{path}")

            # by convention, paths that end with '/' are directories
            if path.endswith("/"):
                with open("/tmp/data.tar.gz", "wb+") as f:
                    f.write(payload)
                subprocess.run(f"mkdir -p {path}", shell=True, check=True)
                subprocess.run(f"tar -xvzf /tmp/data.tar.gz -C {path}", shell=True, check=True)
            else:
                basepath = os.path.split(path)[0]
                subprocess.run(f"mkdir -p {basepath}", shell=True, check=True)
                with open(path, "wb+") as f:
                    f.write(payload)
    return os.listdir()


def list_files(client: Client) -> List[str]:
    """List all files that are being tracked in the file registry"""
    datasets = client.list_datasets()
    return [p[len(PREFIX) :] for p in datasets if p.startswith(PREFIX)]


def clear_files(client: Client):
    """Clear all files that are being tracked in the file registry.

    After this is run, any new worker that is spun up, won't have any files
    automatically registered even if the RegisterFiles plugin is in use.
    """
    paths = list_files(client)
    for path in paths:
        client.unpublish_dataset(path)


def sync_files(client: Client, path: Optional[str] = None):
    """Upload files to all workers and add to file registry.

    :param client: distributed.Client object
    :param path: string or path obj pointing to file or directory to track.

    If used in conjunction with the ``RegisterFiles`` plugin, all files will be uploaded
    to new workers as they get spun up.
    """
    # normalize the path
    path = os.path.abspath(path)

    if os.path.isdir(path):
        path += "/"
        subprocess.run(
            f"tar --exclude .git -cvzf /tmp/data.tar.gz -C {path} .", shell=True, check=True
        )
        with open("/tmp/data.tar.gz", "rb") as f:
            payload = f.read()
    else:
        with open(path, "rb") as f:
            payload = f.read()

    # erase the given file or any file in the directory
    for p in [p for p in client.list_datasets() if path in p]:
        client.unpublish_dataset(p)

    client.publish_dataset(**{f"{PREFIX}{path}": payload})
    client.run(register_files_to_worker, paths=[path])


class RegisterFiles:
    """WorkerPlugin for uploading files or directories to dask workers.

    Use ``sync_files`` to control which paths are tracked.
    """

    name = "register_files"

    # pylint: disable=no-self-use,unused-argument
    async def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        await register_files_to_worker()
