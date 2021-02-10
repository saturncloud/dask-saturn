"""
Saturn-specific override of ``dask.distributed.deploy.SpecCluster``

See https://distributed.dask.org/en/latest/_modules/distributed/deploy/spec.html
for details on the parent class.
"""

import json
import logging

from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import requests

from distributed import Client, SpecCluster
from distributed.security import Security
from tornado.ioloop import PeriodicCallback

from .backoff import ExpBackoff
from .external import ExternalConnection
from .plugins import SaturnSetup
from .settings import Settings


DEFAULT_WAIT_TIMEOUT_SECONDS = 1200

log = logging.getLogger("dask-saturn")
if log.level == logging.NOTSET:
    logging.basicConfig()
    log.setLevel(logging.INFO)


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
    :param external_connection: Configuration for connecting to Saturn Dask from
        outside of the Saturn installation.
    """

    # pylint: disable=unused-argument,super-init-not-called,too-many-instance-attributes

    _sizes = None

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
        external_connection: Optional[Union[Dict[str, str], ExternalConnection]] = None,
        **kwargs,
    ):
        if external_connection:
            if isinstance(external_connection, dict):
                self.external = ExternalConnection(**external_connection)
            else:
                self.external = external_connection
            self.settings = self.external.settings
        else:
            self.external = None
            self.settings = Settings()

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
            self.dask_cluster_id = self.cluster_url.rstrip("/").split("/")[-1]

        info = self._get_info()
        self._dashboard_link = info["dashboard_link"]
        self._scheduler_address = info["scheduler_address"]
        self.loop = None
        self.periodic_callbacks: Dict[str, PeriodicCallback] = {}
        self.autoclose = autoclose
        if self.external:
            self.security = self.external.security(self.dask_cluster_id)
        else:
            self.security = Security()

        try:
            self.register_default_plugin()
        except Exception as e:  # pylint: disable=broad-except
            log.warning(
                f"Registering default plugin failed: {e} Hint: you might "
                "have a different dask-saturn version on your dask cluster."
            )

    def __await__(self):
        async def _():
            pass

        return _().__await__()

    @classmethod
    def reset(
        cls,
        n_workers: Optional[int] = None,
        worker_size: Optional[str] = None,
        worker_is_spot: Optional[bool] = None,
        scheduler_size: Optional[str] = None,
        nprocs: Optional[int] = None,
        nthreads: Optional[int] = None,
        external_connection: Optional[ExternalConnection] = None,
    ) -> "SaturnCluster":
        """Return a SaturnCluster

        Destroy existing Dask cluster attached to the Jupyter Notebook or
        Custom Deployment and recreate it with the given configuration.

        For documentation on this method's parameters, see
        ``help(SaturnCluster)``.
        """
        log.info("Resetting cluster.")
        if external_connection is None:
            settings = Settings()
        else:
            settings = external_connection.settings
        url = urljoin(settings.url, "api/dask_clusters/reset")
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

        response = requests.post(url, data=json.dumps(cluster_config), headers=settings.headers)
        if not response.ok:
            raise ValueError(response.json()["message"])
        return cls(**cluster_config, external_connection=external_connection)

    @property
    def status(self) -> Optional[str]:
        """
        Status of the cluster
        """
        if self.cluster_url is None:
            return "closed"
        url = urljoin(self.cluster_url, "status")
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            return self._get_pod_status()
        return response.json()["status"]

    def _get_pod_status(self) -> Optional[str]:
        """
        Status of the KubeCluster pod.
        """
        response = requests.get(self.cluster_url[:-1], headers=self.settings.headers)
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
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            if self._get_pod_status() in ["error", "closed", "stopped"]:
                for pc in self.periodic_callbacks.values():
                    pc.stop()
                raise ValueError("Cluster is not running.")
            raise ValueError(response.json()["message"])
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
        url = urljoin(self.settings.url, "api/dask_clusters")
        url_query = ""
        if self.external:
            url_query = "?is_external=true"
        self.cluster_url: Optional[str] = None

        self._validate_sizes(worker_size, scheduler_size)

        cluster_config = {
            "n_workers": n_workers,
            "worker_size": worker_size,
            "worker_is_spot": worker_is_spot,
            "scheduler_size": scheduler_size,
            "nprocs": nprocs,
            "nthreads": nthreads,
        }
        if self.external:
            cluster_config["project_id"] = self.external.project_id
        # only send kwargs that are explicity set by user
        cluster_config = {k: v for k, v in cluster_config.items() if v is not None}

        expBackoff = ExpBackoff(wait_timeout=scheduler_service_wait_timeout)
        logged_warnings: Dict[str, bool] = {}
        while self.cluster_url is None:
            response = requests.post(
                url + url_query,
                data=json.dumps(cluster_config),
                headers=self.settings.headers,
            )
            if not response.ok:
                raise ValueError(response.json()["message"])
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
                self.dask_cluster_id = data["id"]
                self.cluster_url = f"{url}/{self.dask_cluster_id}/"
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
        with Client(self) as client:
            output = client.register_worker_plugin(SaturnSetup())
            log.info(output)

    def _get_info(self) -> Dict[str, Any]:
        url = urljoin(self.cluster_url, "info")
        if self.external:
            url += "?is_external=true"
        response = requests.get(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.json()["message"])
        return response.json()

    def scale(self, n: int) -> None:
        """
        Scale cluster to have ``n`` workers

        :param n: number of workers to scale to.
        """
        url = urljoin(self.cluster_url, "scale")
        response = requests.post(url, json.dumps({"n": n}), headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.json()["message"])

    def adapt(self, minimum: int, maximum: int) -> None:
        """Adapt cluster to have between ``minimum`` and ``maximum`` workers"""
        url = urljoin(self.cluster_url, "adapt")
        response = requests.post(
            url,
            json.dumps({"minimum": minimum, "maximum": maximum}),
            headers=self.settings.headers,
        )
        if not response.ok:
            raise ValueError(response.json()["message"])

    def close(self) -> None:
        """
        Defines what should be done when closing the cluster.
        """
        url = urljoin(self.cluster_url, "close")
        response = requests.post(url, headers=self.settings.headers)
        if not response.ok:
            raise ValueError(response.json()["message"])
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

    # pylint: disable=access-member-before-definition
    def _validate_sizes(
        self,
        worker_size: Optional[str] = None,
        scheduler_size: Optional[str] = None,
    ):
        """Validate the options provided"""
        if self._sizes is None:
            self._sizes = list_sizes(self.external)
        errors = []
        if worker_size is not None:
            if worker_size not in self._sizes:
                errors.append(
                    f"Proposed worker_size: {worker_size} is not a valid option. "
                    f"Options are: {self._sizes}."
                )
        if scheduler_size is not None:
            if scheduler_size not in self._sizes:
                errors.append(
                    f"Proposed scheduler_size: {scheduler_size} is not a valid option. "
                    f"Options are: {self._sizes}."
                )
        if len(errors) > 0:
            raise ValueError(" ".join(errors))


def _options(external_connection: Optional[ExternalConnection] = None) -> Dict[str, Any]:
    if external_connection is None:
        settings = Settings()
    else:
        settings = external_connection.settings
    url = urljoin(settings.url, "api/dask_clusters/info")
    response = requests.get(url, headers=settings.headers)
    if not response.ok:
        raise ValueError(response.json()["message"])
    return response.json()["server_options"]


def list_sizes(external_connection: Optional[ExternalConnection] = None) -> List[str]:
    """Return a list of valid size options for worker_size and scheduler size."""
    return [size["name"] for size in _options(external_connection=external_connection)["size"]]


def describe_sizes(external_connection: Optional[ExternalConnection] = None) -> Dict[str, str]:
    """Return a dict of size options with a description."""
    return {
        size["name"]: size["display"]
        for size in _options(external_connection=external_connection)["size"]
    }
