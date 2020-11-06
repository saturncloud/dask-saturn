"""
Worker plugins to be used with SaturnCluster objects.
"""
import os
import subprocess

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


async def _register_files(paths=None):
    """Register all files in the given paths on the current worker"""
    with get_client() as client:
        # If paths isn't provided, register all files in datasets that start with prefix
        if paths is None:
            paths = [p[len(PREFIX) :] for p in await client.list_datasets() if p.startswith(PREFIX)]

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


def upload_files_to_workers(client, path):
    """Upload files to all workers. Single files or directories are valid."""
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

    # update files on any existing workers
    client.run(_register_files, paths=[path])


class UploadFiles:
    """WorkerPlugin for uploading files or directories to dask workers."""

    name = "upload_files"

    # pylint: disable=no-self-use
    async def setup(self, worker=None):
        """This method gets called at worker setup for new and existing workers"""
        await _register_files()
