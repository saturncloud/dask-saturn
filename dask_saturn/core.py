import os
import time
import requests
import json
from urllib.parse import urljoin
from distributed import SpecCluster


SATURN_TOKEN = os.environ.get("SATURN_TOKEN", "")
BASE_URL = os.environ.get("BASE_URL", "")
HEADERS = {"Authorization": f"token {SATURN_TOKEN}"}


class SaturnCluster(SpecCluster):
    def __init__(self, cluster_url=None, *args, **kwargs):
        if cluster_url is None:
            self._start(retries=10, sleep=10)
        else:
            self.cluster_url = cluster_url if cluster_url.endswith("/") else cluster_url + "/"
        info = self._get_info()
        self._dashboard_link = info["dashboard_link"]
        self._scheduler_address = info["scheduler_address"]
        self.loop = None
        self.periodic_callbacks = {}

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

    def _start(self, retries=0, sleep=0):
        """Start a cluster that has already been defined for the project"""
        url = urljoin(BASE_URL, "api/dask_clusters")
        max_retries = retries
        self.cluster_url = None

        while self.cluster_url is None:
            response = requests.post(url, headers=HEADERS)
            if not response.ok:
                raise ValueError(response.reason)
            data = response.json()
            if data["status"] == "error":
                raise ValueError(" ".join(data["errors"]))
            elif data["status"] == "ready":
                self.cluster_url = f"{url}/{data['id']}/"
            else:
                print(f"Starting cluster. Status: {data['status']}")

            if self.cluster_url is None:
                if retries <= 0:
                    raise ValueError(
                        "Retry in a few minutes. Check status in Saturn User Interface"
                    )
                retries -= 1
                time.sleep(sleep)

        if 0 < retries < max_retries:
            print("Cluster is ready")

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
