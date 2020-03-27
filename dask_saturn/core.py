import os
import requests
import json
from urllib.parse import urljoin
from distributed import SpecCluster


SATURN_TOKEN = os.environ.get("SATURN_TOKEN", "")
BASE_URL = os.environ.get("BASE_URL", "")
HEADERS = {"Authorization": f"token {SATURN_TOKEN}"}


def start():
    clusters_url = urljoin(BASE_URL, "api/dask_clusters/")
    response = requests.post(clusters_url, headers=HEADERS)
    name = response.json()["name"]
    return urljoin(cluster_url, name)


class SaturnCluster(SpecCluster):
    def __init__(self, cluster_url=None):
        if cluster_url is None:
            cluster_url = start()
        self.cluster_url = cluster_url
        info = self._get_info()
        self._dashboard_link = info["dashboard_link"]
        self._scheduler_address = info["scheduler_address"]
        self.loop = None
        self.periodic_callbacks = {}

    @property
    def status(self):
        url = urljoin(self.cluster_url, "status")
        return requests.get(url, headers=HEADERS).content.decode()

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
        return requests.get(url, headers=HEADERS).json()

    def _get_info(self):
        url = urljoin(self.cluster_url, "info")
        return requests.get(url, headers=HEADERS).json()

    def scale(self, n):
        url = urljoin(self.cluster_url, "scale")
        requests.post(url, json.dumps({"n": n}), headers=HEADERS)

    def adapt(self, minimum, maximum):
        url = urljoin(self.cluster_url, "adapt")
        requests.post(url, json.dumps({"minimum": minimum, "maximum": maximum}), headers=HEADERS)

    def close(self):
        response = requests.patch(self.cluster_url, json.dumps({"operation": "stop"}), headers=HEADERS)
        return response.json()
