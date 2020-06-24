import os
import requests
import json
import logging

from urllib.parse import urljoin
from distributed import SpecCluster
from typing import List, Dict
from sys import stdout

from .backoff import ExpBackoff

try:
    SATURN_TOKEN = os.environ["SATURN_TOKEN"]
except KeyError:
    raise RuntimeError(
        "Required environment variable SATURN_TOKEN not set. "
        "dask-saturn code should only be run on Saturn Cloud infrastructure."
    )

try:
    BASE_URL = os.environ["BASE_URL"]
except KeyError:
    raise RuntimeError(
        "Required environment variable BASE_URL not set. "
        "dask-saturn code should only be run on Saturn Cloud infrastructure."
    )

HEADERS = {"Authorization": f"token {SATURN_TOKEN}"}
DEFAULT_WAIT_TIMEOUT_SECONDS = 1200

logfmt = "[%(asctime)s] %(levelname)s - %(name)s | %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"

log = logging.getLogger('dask-saturn')
log.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(logfmt, datefmt))
log.addHandler(handler)


class SaturnCluster(SpecCluster):
    def __init__(
        self,
        *args,
        n_workers=None,
        cluster_url=None,
        worker_size=None,
        scheduler_size=None,
        nprocs=None,
        nthreads=None,
        scheduler_service_wait_timeout=DEFAULT_WAIT_TIMEOUT_SECONDS,
        autoclose=False,
        **kwargs,
    ):
        if cluster_url is None:
            self._start(
                n_workers=n_workers,
                worker_size=worker_size,
                scheduler_size=scheduler_size,
                nprocs=nprocs,
                nthreads=nthreads,
                scheduler_service_wait_timeout=scheduler_service_wait_timeout,
            )
        else:
            self.cluster_url = cluster_url if cluster_url.endswith("/") else cluster_url + "/"
        info = self._get_info()
        self._dashboard_link = info["dashboard_link"]
        self._scheduler_address = info["scheduler_address"]
        self.loop = None
        self.periodic_callbacks = {}
        self.autoclose = autoclose

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
        """Return a SaturnCluster

        Destroy existing Dask cluster attached to the Jupyter Notebook or
        Custom Deployment and recreate it with the given configuration.
        """
        log.info("Resetting cluster.")
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
        return cls(**cluster_config)

    @property
    def status(self):
        if self.cluster_url is None:
            return "closed"
        url = urljoin(self.cluster_url, "status")
        response = requests.get(url, headers=HEADERS)
        if not response.ok:
            return self._get_pod_status()
        return response.json()["status"]

    def _get_pod_status(self):
        response = requests.get(self.cluster_url[:-1], headers=HEADERS)
        if response.ok:
            return response.json()["status"]

    @property
    def _supports_scaling(self):
        return True

    @property
    def scheduler_address(self):
        return self._scheduler_address

    @property
    def dashboard_link(self):
        return self._dashboard_link

    @property
    def scheduler_info(self):
        url = urljoin(self.cluster_url, "scheduler_info")
        response = requests.get(url, headers=HEADERS)
        if not response.ok:
            if self._get_pod_status() in ["error", "closed", "stopped"]:
                for pc in self.periodic_callbacks.values():
                    pc.stop()
                raise ValueError("Cluster is not running.")
            raise ValueError(response.reason)
        return response.json()

    def _start(
        self,
        n_workers=None,
        worker_size=None,
        scheduler_size=None,
        nprocs=None,
        nthreads=None,
        scheduler_service_wait_timeout=DEFAULT_WAIT_TIMEOUT_SECONDS,
    ):
        """Start a cluster that has already been defined for the project."""
        url = urljoin(BASE_URL, "api/dask_clusters")
        self.cluster_url = None

        cluster_config = {
            "n_workers": n_workers,
            "worker_size": worker_size,
            "scheduler_size": scheduler_size,
            "nprocs": nprocs,
            "nthreads": nthreads,
        }

        expBackoff = ExpBackoff(wait_timeout=scheduler_service_wait_timeout)
        while self.cluster_url is None:
            response = requests.post(url, data=json.dumps(cluster_config), headers=HEADERS)
            if not response.ok:
                raise ValueError(response.reason)
            data = response.json()
            warnings = data.get("warnings")
            if warnings is not None:
                for warning in warnings:
                    log.warning(warning)
            if data["status"] == "error":
                raise ValueError(" ".join(data["errors"]))
            elif data["status"] == "ready":
                self.cluster_url = f"{url}/{data['id']}/"
                log.info("Cluster is ready")
                break
            else:
                log.info(f"Starting cluster. Status: {data['status']}")

            if self.cluster_url is None:
                if not expBackoff.wait():
                    raise ValueError(
                        "Retry in a few minutes. Check status in Saturn User Interface"
                    )

    def _get_info(self):
        url = urljoin(self.cluster_url, "info")
        response = requests.get(url, headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)
        return response.json()

    def scale(self, n):
        """Scale cluster to have ``n`` workers"""
        url = urljoin(self.cluster_url, "scale")
        response = requests.post(url, json.dumps({"n": n}), headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)

    def adapt(self, minimum, maximum):
        """Adapt cluster to have between ``minimum`` and ``maximum`` workers"""
        url = urljoin(self.cluster_url, "adapt")
        response = requests.post(
            url, json.dumps({"minimum": minimum, "maximum": maximum}), headers=HEADERS
        )
        if not response.ok:
            raise ValueError(response.reason)

    def close(self):
        url = urljoin(self.cluster_url, "close")
        response = requests.post(url, headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)
        for pc in self.periodic_callbacks.values():
            pc.stop()

    @property
    def asynchronous():
        return False

    def __enter__(self):
        assert self.status == "running"
        return self

    def __exit__(self, typ, value, traceback):
        if self.autoclose:
            return self.close()


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
