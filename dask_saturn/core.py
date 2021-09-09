"""
Saturn-specific override of ``dask.distributed.deploy.SpecCluster``

See https://distributed.dask.org/en/latest/_modules/distributed/deploy/spec.html
for details on the parent class.
"""
import os
import json
import logging
import warnings
import weakref

from distutils.version import LooseVersion
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from distributed import Client, SpecCluster
from distributed.security import Security
from tornado.ioloop import PeriodicCallback

from .backoff import ExpBackoff
from .external import _security, ExternalConnection  # noqa  # pylint: disable=unused-import
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
    :param shutdown_on_close: Whether or not the cluster should be automatically destroyed
        when its calling process is destroyed. Set this parameter to ``True`` if you want
        your cluster to shutdown when the work is done.
        By default, this is ``False`` if the cluster is attached to a Jupyter server,
        deployment, or job. If the cluster is attached to a Prefect Cloud flow run, this option
        is always set to ``True``.
        Note: ``autoclose`` is accepted as an alias for now, but will be removed in the future.
    """

    # pylint: disable=unused-argument,super-init-not-called,too-many-instance-attributes

    _sizes = None
    _instances = weakref.WeakSet()

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
        shutdown_on_close: bool = False,
        **kwargs,
    ):
        if "external_connection" in kwargs:
            raise RuntimeError(
                "Passing external_connection as a key word argument is no longer supported. "
                "Instead, set the env vars: ``SATURN_TOKEN`` and ``SATURN_BASE_URL`` "
                "as indicated in the Saturn Cloud UI. If those env vars are set, an external "
                "connection will be automatically set up."
            )
        if "autoclose" in kwargs:
            warnings.warn(
                "``autoclose`` has been deprecated and will be removed in a future version. "
                "Please use ``shutdown_on_close`` instead.",
                category=FutureWarning,
            )
            shutdown_on_close = kwargs.pop("autoclose")

        self.settings = Settings()

        if self.settings.is_prefect:
            # defaults to True if related to prefect, else defaults to False
            shutdown_on_close = True

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
        self._name = self.dask_cluster_id
        self._dashboard_link = info["dashboard_link"]
        self._scheduler_address = info["scheduler_address"]
        self.loop = None
        self.periodic_callbacks: Dict[str, PeriodicCallback] = {}
        self.shutdown_on_close = shutdown_on_close
        self._adaptive = None
        self._instances.add(self)

        if self.settings.is_external:
            self.security = _security(self.settings, self.dask_cluster_id)
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
    ) -> "SaturnCluster":
        """Return a SaturnCluster

        Destroy existing Dask cluster attached to the Jupyter Notebook or
        Custom Deployment and recreate it with the given configuration.

        For documentation on this method's parameters, see
        ``help(SaturnCluster)``.
        """
        log.info("Resetting cluster.")
        settings = Settings()
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
        return cls(**cluster_config)

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
        try:
            from distributed.objects import SchedulerInfo  # pylint: disable=import-outside-toplevel

            return SchedulerInfo(response.json())
        except ImportError:
            pass
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
        if self.settings.is_external:
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

        if self.settings.SATURN_VERSION >= LooseVersion("2021.08.16"):
            cluster_config["prefectcloudflowrun_id"] = os.environ.get(
                "PREFECT__CONTEXT__FLOW_RUN_ID"
            )

        # only send kwargs that are explicitly set by user
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
            for warning in data.get("warnings", []):
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
        outputs = {}
        with Client(self) as client:
            output = client.register_worker_plugin(SaturnSetup())
            outputs.update(output)
        output_statuses = [v["status"] for v in outputs.values()]
        if "OK" in output_statuses:
            log.info("Success!")
        elif "repeat" in output_statuses:
            log.info("Success!")
        elif len(output_statuses) == 0:
            log.warning("No workers started up.")
        else:
            log.warning("Registering default plugins failed. Please check logs for more info.")

    def _get_info(self) -> Dict[str, Any]:
        url = urljoin(self.cluster_url, "info")
        if self.settings.is_external:
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
        if self.shutdown_on_close:
            self.close()

    # pylint: disable=access-member-before-definition
    def _validate_sizes(
        self,
        worker_size: Optional[str] = None,
        scheduler_size: Optional[str] = None,
    ):
        """Validate the options provided"""
        if self._sizes is None:
            self._sizes = list_sizes()
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

    @classmethod
    def from_name(cls, name: str):
        """Create an instance of this class to represent an existing cluster by name."""
        log.warning(
            "Only one dask cluster can be associated with a particular resource, so "
            f"user provided name: {name} will not be used."
        )
        return cls()


def _options() -> Dict[str, Any]:
    settings = Settings()
    url = urljoin(settings.url, "api/dask_clusters/info")
    response = requests.get(url, headers=settings.headers)
    if not response.ok:
        raise ValueError(response.json()["message"])
    return response.json()["server_options"]


def list_sizes() -> List[str]:
    """Return a list of valid size options for worker_size and scheduler size."""
    return [size["name"] for size in _options()["size"]]


def describe_sizes() -> Dict[str, str]:
    """Return a dict of size options with a description."""
    return {size["name"]: size["display"] for size in _options()["size"]}
