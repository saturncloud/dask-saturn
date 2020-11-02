"""
Saturn-specific override of ``dask.distributed.deploy.SpecCluster``

See https://distributed.dask.org/en/latest/_modules/distributed/deploy/spec.html
for details on the parent class.
"""

import os
import json
import logging

from sys import stdout
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from distributed import SpecCluster
from distributed.worker import get_client
from tornado.ioloop import PeriodicCallback


from .backoff import ExpBackoff
from .plugins import SaturnSetup


DEFAULT_WAIT_TIMEOUT_SECONDS = 1200

logfmt = "[%(asctime)s] %(levelname)s - %(name)s | %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"

log = logging.getLogger("dask-saturn")
log.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(logfmt, datefmt))
log.addHandler(handler)


def _get_base_url():
    """Retrieve env var BASE_URL, throwing error if not available"""
    try:
        return os.environ["BASE_URL"]
    except KeyError as err:
        raise RuntimeError(
            "Required environment variable BASE_URL not set. "
            "dask-saturn code should only be run on Saturn Cloud infrastructure."
        ) from err


def _get_headers():
    """Retrive env var SATURN_TOKEN, throwing error if not available and create headers"""
    try:
        SATURN_TOKEN = os.environ["SATURN_TOKEN"]
    except KeyError as err:
        raise RuntimeError(
            "Required environment variable SATURN_TOKEN not set. "
            "dask-saturn code should only be run on Saturn Cloud infrastructure."
        ) from err
    return {"Authorization": f"token {SATURN_TOKEN}"}


class SaturnCluster(SpecCluster):
    """
    Create a ``SaturnCluster``, an extension of ``distributed.SpecCluster``
    specific to Saturn Cloud.

    :param n_workers: Number of workers to provision for the cluster.
    :param cluster_url: URL for the "cluster" service running in kubernetes. This
        component of a ``Dask`` cluster in kubernetes knows how to create pods
        for workers and the scheduler.
    :param worker_size: A string with the size to use for each worker. A list of
        valid sizes and their details can be obtained with ``dask_saturn.describe_sizes()``.
        If no size is provided, this will default to the size configured for Jupyter
        in your Saturn Cloud project.
    :param worker_is_spot: Flag to indicate if workers should be started on Spot Instances nodes.
        Added in dask-saturn 0.1.0, Saturn 2020.08.28.
    :param scheduler_size: A string with the size to use for the scheduler. A list of
        valid sizes and their details can be obtained with ``dask_saturn.describe_sizes()``.
        If no size is provided, this will default to the size configured for Jupyter
        in your Saturn Cloud project.
    :param nprocs: The number of ``dask-worker`` processes run on each host in a distributed
        cluster.
    :param nthreads: The number of threads available to each ``dask-worker`` process.
    :param scheduler_service_wait_timeout: The maximum amout of time, in seconds, that
        ``SaturnCluster`` will wait for the scheduler to respond before deciding
        that the scheduler is taking too long. By default, this is set to 1200 (20
        minutes). Setting it to a lower value will help you catch problems earlier,
        but may also lead to false positives if you don't give the cluster
        enough to time to start up.
    :param autoclose: Whether or not the cluster should be automatically destroyed
        when its ``__exit__()`` method is called. By default, this is ``False``. Set
        this parameter to ``True`` if you want to use it in a context manager which
        closes the cluster when it exits.
    """

    # pylint: disable=unused-argument,super-init-not-called
    def __init__(
        self,
        *args,
        n_workers: Optional[int] = None,
        cluster_url: Optional[str] = None,
        worker_size: Optional[str] = None,
        worker_is_spot: Optional[bool] = None,
        scheduler_size: Optional[str] = None,
        nprocs: Optional[int] = None,
        nthreads: Optional[int] = None,
        scheduler_service_wait_timeout: int = DEFAULT_WAIT_TIMEOUT_SECONDS,
        autoclose: bool = False,
        **kwargs,
    ):
        if cluster_url is None:
            self._start(
                n_workers=n_workers,
                worker_size=worker_size,
                worker_is_spot=worker_is_spot,
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
        self.periodic_callbacks: Dict[str, PeriodicCallback] = {}
        self.autoclose = autoclose
        try:
            self.register_default_plugin()
        except Exception as e:  # pylint: disable=broad-except
            log.warning(
                f"Registering default plugin failed: {e} Hint: you might "
                "have a different dask-saturn version on your dask cluster."
            )

    @classmethod
    def reset(
        cls,
        n_workers: Optional[int] = None,
        worker_size: Optional[str] = None,
        worker_is_spot: Optional[bool] = None,
        scheduler_size: Optional[str] = None,
        nprocs: Optional[int] = None,
        nthreads: Optional[int] = None,
    ) -> "SaturnCluster":
        """Return a SaturnCluster

        Destroy existing Dask cluster attached to the Jupyter Notebook or
        Custom Deployment and recreate it with the given configuration.

        For documentation on this method's parameters, see
        ``help(SaturnCluster)``.
        """
        log.info("Resetting cluster.")
        url = urljoin(_get_base_url(), "api/dask_clusters/reset")
        cluster_config = {
            "n_workers": n_workers,
            "worker_size": worker_size,
            "worker_is_spot": worker_is_spot,
            "scheduler_size": scheduler_size,
            "nprocs": nprocs,
            "nthreads": nthreads,
        }
        # only send kwargs that are explicity set by user
        cluster_config = {k: v for k, v in cluster_config.items() if v is not None}

        response = requests.post(url, data=json.dumps(cluster_config), headers=_get_headers())
        if not response.ok:
            raise ValueError(response.reason)
        return cls(**cluster_config)

    @property
    def status(self) -> Optional[str]:
        """
        Status of the cluster
        """
        if self.cluster_url is None:
            return "closed"
        url = urljoin(self.cluster_url, "status")
        response = requests.get(url, headers=_get_headers())
        if not response.ok:
            return self._get_pod_status()
        return response.json()["status"]

    def _get_pod_status(self) -> Optional[str]:
        """
        Status of the KubeCluster pod.
        """
        response = requests.get(self.cluster_url[:-1], headers=_get_headers())
        if response.ok:
            return response.json()["status"]
        else:
            return None

    @property
    def _supports_scaling(self) -> bool:
        """
        Property required by ``SpecCluster``, which describes
        whether the cluster can be scaled after it's created.
        """
        return True

    @property
    def scheduler_address(self) -> str:
        """
        Address for the Dask schduler.
        """
        return self._scheduler_address

    @property
    def dashboard_link(self) -> str:
        """
        Link to the Dask dashboard. This is customized
        to be inside of a Saturn project.
        """
        return self._dashboard_link

    @property
    def scheduler_info(self) -> Dict[str, Any]:
        """
        Information about the scheduler. Raises a
        ValueError if the scheduler is in a bad state.
        """
        url = urljoin(self.cluster_url, "scheduler_info")
        response = requests.get(url, headers=_get_headers())
        if not response.ok:
            if self._get_pod_status() in ["error", "closed", "stopped"]:
                for pc in self.periodic_callbacks.values():
                    pc.stop()
                raise ValueError("Cluster is not running.")
            raise ValueError(response.reason)
        return response.json()

    # pylint: disable=invalid-overridden-method
    def _start(
        self,
        n_workers: Optional[int] = None,
        worker_size: Optional[str] = None,
        worker_is_spot: Optional[bool] = None,
        scheduler_size: Optional[str] = None,
        nprocs: Optional[int] = None,
        nthreads: Optional[int] = None,
        scheduler_service_wait_timeout: int = DEFAULT_WAIT_TIMEOUT_SECONDS,
    ) -> None:
        """
        Start a cluster that has already been defined for the project.

        For documentation on this method's parameters, see
        ``help(SaturnCluster)``.
        """
        url = urljoin(_get_base_url(), "api/dask_clusters")
        self.cluster_url: Optional[str] = None

        cluster_config = {
            "n_workers": n_workers,
            "worker_size": worker_size,
            "worker_is_spot": worker_is_spot,
            "scheduler_size": scheduler_size,
            "nprocs": nprocs,
            "nthreads": nthreads,
        }
        # only send kwargs that are explicity set by user
        cluster_config = {k: v for k, v in cluster_config.items() if v is not None}

        expBackoff = ExpBackoff(wait_timeout=scheduler_service_wait_timeout)
        logged_warnings: Dict[str, bool] = {}
        while self.cluster_url is None:
            response = requests.post(url, data=json.dumps(cluster_config), headers=_get_headers())
            if not response.ok:
                raise ValueError(response.reason)
            data = response.json()
            warnings = data.get("warnings")
            if warnings is not None:
                for warning in warnings:
                    if not logged_warnings.get(warning):
                        logged_warnings[warning] = True
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

    def register_default_plugin(self):
        """Register the default SaturnSetup plugin to all workers."""
        log.info("Registering default plugins")
        with get_client(self.scheduler_address) as client:
            output = client.register_worker_plugin(SaturnSetup())
            log.info(output)

    def _get_info(self) -> Dict[str, Any]:
        url = urljoin(self.cluster_url, "info")
        response = requests.get(url, headers=_get_headers())
        if not response.ok:
            raise ValueError(response.reason)
        return response.json()

    def scale(self, n: int) -> None:
        """
        Scale cluster to have ``n`` workers

        :param n: number of workers to scale to.
        """
        url = urljoin(self.cluster_url, "scale")
        response = requests.post(url, json.dumps({"n": n}), headers=_get_headers())
        if not response.ok:
            raise ValueError(response.reason)

    def adapt(self, minimum: int, maximum: int) -> None:
        """Adapt cluster to have between ``minimum`` and ``maximum`` workers"""
        url = urljoin(self.cluster_url, "adapt")
        response = requests.post(
            url, json.dumps({"minimum": minimum, "maximum": maximum}), headers=_get_headers()
        )
        if not response.ok:
            raise ValueError(response.reason)

    def close(self) -> None:
        """
        Defines what should be done when closing the cluster.
        """
        url = urljoin(self.cluster_url, "close")
        response = requests.post(url, headers=_get_headers())
        if not response.ok:
            raise ValueError(response.reason)
        for pc in self.periodic_callbacks.values():
            pc.stop()

    @property
    def asynchronous(self) -> bool:
        """
        Whether or not the cluster's ``_start`` method
        is synchronous.

        ``SaturnCluster`` uses a synchronous ``_start()``
        because it has to be called in the class
        constructor, which is intended to be used interactively
        in a notebook.
        """
        return False

    def __enter__(self) -> "SaturnCluster":
        """
        magic method used to allow the use of ``SaturnCluster``
        with a context manager.

        .. code-block:: python

            with SaturnCluster() as cluster:
        """
        return self

    def __exit__(self, typ, value, traceback) -> None:
        """
        magic method that defines what should be done
        when exiting a context manager's context. in other words
        at the end of this

        .. code-block:: python

            with SaturnCluster() as cluster:
        """
        if self.autoclose:
            self.close()


def _options() -> Dict[str, Any]:
    url = urljoin(_get_base_url(), "api/dask_clusters/info")
    response = requests.get(url, headers=_get_headers())
    if not response.ok:
        raise ValueError(response.reason)
    return response.json()["server_options"]


def list_sizes() -> List[str]:
    """Return a list of valid size options for worker_size and scheduler size."""
    return [size["name"] for size in _options()["size"]]


def describe_sizes() -> Dict[str, str]:
    """Return a dict of size options with a description."""
    return {size["name"]: size["display"] for size in _options()["size"]}
