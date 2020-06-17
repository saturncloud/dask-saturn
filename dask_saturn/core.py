import os
import requests
import json
import asyncio
import weakref
import logging

from urllib.parse import urljoin
from distributed import SpecCluster
from distributed.utils import LoopRunner
from typing import List, Dict

from .backoff import ExpBackoff


logger = logging.getLogger(__name__)


SATURN_TOKEN = os.environ.get("SATURN_TOKEN", "")
BASE_URL = os.environ.get("BASE_URL", "")
HEADERS = {"Authorization": f"token {SATURN_TOKEN}"}
DEFAULT_WAIT_TIMEOUT_SECONDS = 1200


class _STATUS:
    """ Statuses can come from Saturn or from dask-kubernetes"""

    CREATED = "created"
    STARTING = "starting"
    READY = "ready"
    RUNNING = "running"
    CLOSING = "closing"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CLOSED = "closed"
    ERROR = "error"


class SaturnCluster(SpecCluster):
    cluster_url = None
    _scheduler_address = None
    _dashboard_link = None
    _instances = weakref.WeakSet()

    def __init__(
        self,
        *args,
        n_workers=0,
        worker_size=None,
        scheduler_size=None,
        nprocs=1,
        nthreads=1,
        scheduler_service_wait_timeout=DEFAULT_WAIT_TIMEOUT_SECONDS,
        autoclose=False,
        loop=None,
        asynchronous=False,
        **kwargs,
    ):
        self.n_workers = n_workers
        self.worker_size = worker_size
        self.scheduler_size = scheduler_size
        self.nprocs = nprocs
        self.nthreads = nthreads
        self.scheduler_service_wait_timeout = scheduler_service_wait_timeout
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop
        self.periodic_callbacks = {}
        self._lock = asyncio.Lock()
        self._asynchronous = asynchronous
        self._instances.add(self)
        self._correct_state_waiting = None
        self.autoclose = autoclose
        self.status = _STATUS.CREATED

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)
            expBackoff = ExpBackoff(wait_timeout=self.scheduler_service_wait_timeout)
            while self.status in [_STATUS.CREATED, _STATUS.STARTING]:
                expBackoff.wait(asynchronous=False)
                self._refresh_status()
                logger.info(f"Cluster is {self.status}")

    def _refresh_status(self):
        url = urljoin(self.cluster_url, "status")
        response = requests.get(url, headers=HEADERS)
        if response.ok:
            self.status = response.json()["status"]
        else:
            self._get_pod_status()

    def _get_pod_status(self):
        response = requests.get(self.cluster_url[:-1], headers=HEADERS)
        if response.ok:
            data = response.json()
            self.status = data["status"]
        else:
            self.status = _STATUS.CLOSED

    @property
    def _supports_scaling(self):
        return True

    @property
    def scheduler_address(self):
        return self._scheduler_address

    @property
    def dashboard_link(self):
        return self._dashboard_link

    def __await__(self):
        async def _():
            if self.status == _STATUS.CREATED:
                await self._start()
            return self

        return _().__await__()

    @property
    def scheduler_info(self):
        if self.cluster_url is None:
            return {"workers": {}}
        url = urljoin(self.cluster_url, "scheduler_info")
        response = requests.get(url, headers=HEADERS)
        if not response.ok:
            self._refresh_status()
            if self.status in [_STATUS.STOPPED, _STATUS.CLOSED, _STATUS.ERROR]:
                for pc in self.periodic_callbacks.values():
                    pc.stop()
            if self.status == _STATUS.ERROR:
                raise ValueError("Cluster is not running.")
            return {"workers": {}}
        return response.json()

    def __enter__(self):
        self.status = _STATUS.STARTING
        self.sync(self._start)
        assert self.status in [_STATUS.RUNNING, _STATUS.READY]
        return self

    async def _start(self):
        """Start a cluster"""
        expBackoff = ExpBackoff(wait_timeout=self.scheduler_service_wait_timeout)

        while self.status == _STATUS.STARTING:
            await expBackoff.wait()
            if self.cluster_url is not None:
                self._refresh_status()
                logger.info(f"Cluster is {self.status}")
            else:
                break

        if self.status in [_STATUS.RUNNING, _STATUS.READY]:
            logger.info(f"Cluster is {self.status}")
            return
        if self.status == _STATUS.CLOSED:
            raise ValueError(f"Cluster is {self.status}")

        while self.status in [_STATUS.CLOSING, _STATUS.STOPPING]:
            expBackoff.wait(asynchronous=False)
            if self.cluster_url is not None:
                self._refresh_status()
            else:
                break

        self.status = _STATUS.STARTING
        logger.info(f"Starting cluster")

        cluster_config = {
            "n_workers": self.n_workers,
            "worker_size": self.worker_size,
            "scheduler_size": self.scheduler_size,
            "nprocs": self.nprocs,
            "nthreads": self.nthreads,
        }

        url = urljoin(BASE_URL, "api/dask_clusters")
        response = requests.post(url, data=json.dumps(cluster_config), headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)
        data = response.json()

        self.status = data["status"]
        if self.status == _STATUS.ERROR:
            raise ValueError(" ".join(data["errors"]))

        self.cluster_url = f"{url}/{data['id']}/"
        self._dashboard_link = data["dashboard_address"]
        self._scheduler_address = data["scheduler_address"]
        self._refresh_status()

    @classmethod
    def reset(
        cls,
        n_workers=None,
        worker_size=None,
        scheduler_size=None,
        nprocs=None,
        nthreads=None,
        scheduler_service_wait_timeout=DEFAULT_WAIT_TIMEOUT_SECONDS,
    ):
        """
        Destroy existing Dask cluster attached to the Saturn resource
        and recreate it with the given configuration.
        """
        logger.info(f"Resetting cluster.")
        url = urljoin(BASE_URL, "api/dask_clusters/reset")
        cluster_config = {
            "n_workers": n_workers,
            "worker_size": worker_size,
            "scheduler_size": scheduler_size,
            "nprocs": nprocs,
            "nthreads": nthreads,
        }
        response = requests.post(url, data=json.dumps(cluster_config), headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)
        return cls()

    def scale(self, n):
        """Scale cluster to have ``n`` workers"""
        self._refresh_status()
        assert self.status == _STATUS.RUNNING

        url = urljoin(self.cluster_url, "scale")
        response = requests.post(url, json.dumps({"n": n}), headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)

    def adapt(self, minimum, maximum):
        """Adapt cluster to have between ``minimum`` and ``maximum`` workers"""
        self._refresh_status()
        assert self.status == _STATUS.RUNNING

        url = urljoin(self.cluster_url, "adapt")
        response = requests.post(
            url, json.dumps({"minimum": minimum, "maximum": maximum}), headers=HEADERS
        )
        if not response.ok:
            raise ValueError(response.reason)

    async def _close(self):
        expBackoff = ExpBackoff(wait_timeout=self.scheduler_service_wait_timeout)

        while self.status in [_STATUS.CLOSING, _STATUS.STOPPING]:
            await expBackoff.wait()
            self._refresh_status()
        if self.status in [_STATUS.STOPPED, _STATUS.CLOSED]:
            return
        self.status = _STATUS.CLOSING

        url = urljoin(self.cluster_url, "close")
        response = requests.post(url, headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)
        for pc in self.periodic_callbacks.values():
            pc.stop()

    def close(self, timeout=None):
        # let close fail if there is a RunTimeError - this means the process was killed
        return self.sync(self._close, callback_timeout=timeout)

    def __exit__(self, typ, value, traceback):
        if self.autoclose:
            self.close()
            self._loop_runner.stop()

    async def __aexit__(self, typ, value, traceback):
        if self.autoclose:
            await self.close()


def _options():
    url = urljoin(BASE_URL, "api/dask_clusters/info")
    response = requests.get(url, headers=HEADERS)
    if not response.ok:
        raise ValueError(response.reason)
    return response.json()["server_options"]


def list_sizes() -> List[str]:
    """Return a list of valid size options for worker_size and scheduler size."""
    return [size["name"] for size in _options()["size"]]


def describe_sizes() -> Dict[str, str]:
    """Return a dict of size options with a description."""
    return {size["name"]: size["display"] for size in _options()["size"]}
