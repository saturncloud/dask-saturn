import os
import time
import requests
import json

from datetime import datetime
from random import randrange
from urllib.parse import urljoin
from distributed import SpecCluster


SATURN_TOKEN = os.environ.get("SATURN_TOKEN", "")
BASE_URL = os.environ.get("BASE_URL", "")
HEADERS = {"Authorization": f"token {SATURN_TOKEN}"}
DEFAULT_WAIT_TIMEOUT_SECONDS=1200


class SaturnCluster(SpecCluster):
    def __init__(self, cluster_url=None, *args, **kwargs):
        if cluster_url is None:
            self._start(**kwargs)
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

    def _start(self, **kwargs):
        """Start a cluster that has already been defined for the project"""
        url = urljoin(BASE_URL, "api/dask_clusters")
        self.cluster_url = None

        cluster_config = {
            "worker_size": kwargs.get("worker_size"),
            "scheduler_size": kwargs.get("scheduler_size"),
            "nprocs": kwargs.get("nprocs"),
            "nthreads": kwargs.get("nthreads"),
        }

        wait_timeout = kwargs.get("scheduler_service_wait_timeout", DEFAULT_WAIT_TIMEOUT_SECONDS)
        expBackoff = ExpBackoff(wait_timeout=wait_timeout)
        while self.cluster_url is None:
            response = requests.post(url, data=json.dumps(cluster_config), headers=HEADERS)
            if not response.ok:
                raise ValueError(response.reason)
            data = response.json()
            if data["status"] == "error":
                raise ValueError(" ".join(data["errors"]))
            elif data["status"] == "ready":
                self.cluster_url = f"{url}/{data['id']}/"
                print("Cluster is ready")
                break
            else:
                print(f"Starting cluster. Status: {data['status']}")

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

class ExpBackoff:
    def __init__(self, wait_timeout=1200, min_sleep=5, max_sleep=60):
        self.wait_timeout = wait_timeout
        self.max_sleep = max_sleep
        self.min_sleep = min_sleep
        self.retries = 0
    
    def wait(self):
        if self.retries == 0:
            self.start_time = datetime.now()

        # Check if timeout has been reached
        time_delta = (datetime.now() - self.start_time).total_seconds()
        if time_delta >= self.wait_timeout:
            return False

        # Generate exp backoff with jitter
        self.retries += 1
        backoff = min(self.max_sleep, self.min_sleep * 2 ** self.retries) / 2
        jitter = randrange(0, backoff)
        wait_time = backoff + jitter

        # Make sure we aren't waiting longer than wait_timeout
        remaining_time = self.wait_timeout - time_delta
        if remaining_time < wait_time:
            wait_time = remaining_time

        time.sleep(wait_time)
        return True
