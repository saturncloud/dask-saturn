import os
import requests
import json
import asyncio
from urllib.parse import urljoin
from distributed import SpecCluster
from distributed.utils import LoopRunner


SATURN_TOKEN = os.environ.get("SATURN_TOKEN", "")
BASE_URL = os.environ.get("BASE_URL", "")
HEADERS = {"Authorization": f"token {SATURN_TOKEN}"}


class SaturnCluster(SpecCluster):
    cluster_url = None
    _scheduler_address = None
    _dashboard_link = None

    def __init__(self, loop=None, asynchronous=False, **kwargs):
        if len(self._instances) >= 1:
            i = [i for i in self._instances][0]
            raise KeyError(
                "Cannot start new cluster. " f"Cluster already exists at: {i.scheduler_address}."
            )
        else:
            self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
            self.loop = self._loop_runner.loop
            self.periodic_callbacks = {}
            self._lock = asyncio.Lock()
            self._asynchronous = asynchronous
            self._instances.add(self)
        self.status = "created"

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

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
            self.status = "closed"

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
            if self.status == "created":
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
            if self.status in ["error", "closed", "stopped"]:
                for pc in self.periodic_callbacks.values():
                    pc.stop()
            if self.status == "error":
                raise ValueError("Cluster is not running.")
            return {"workers": {}}
        return response.json()

    async def _start(self):
        """Start a cluster"""
        while self.status == "starting":
            await asyncio.sleep(1)
            print(f"Starting cluster. Status: {self.status}")
            if self.cluster_url is not None:
                self._refresh_status()
        if self.status == "running":
            return
        if self.status == "closed":
            raise ValueError("Cluster is closed")

        self.status = "starting"

        url = urljoin(BASE_URL, "api/dask_clusters")
        response = requests.post(url, headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)
        data = response.json()

        self.status = data["status"]
        if self.status == "error":
            raise ValueError(" ".join(data["errors"]))

        self.cluster_url = f"{url}/{data['id']}/"
        self._dashboard_link = data["dashboard_address"]
        self._scheduler_address = data["scheduler_address"]
        self._refresh_status()

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

    async def _close(self):
        while self.status == "closing":
            await asyncio.sleep(1)
            self._refresh_status()
        if self.status in ["stopped", "closed"]:
            return
        self.status = "closing"

        url = urljoin(self.cluster_url, "close")
        response = requests.post(url, headers=HEADERS)
        if not response.ok:
            raise ValueError(response.reason)
        for pc in self.periodic_callbacks.values():
            pc.stop()
