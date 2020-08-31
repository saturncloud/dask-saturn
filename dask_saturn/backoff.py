"""
Lightweight implementation of exponential backoff,
used for operations that require polling. This is simple enough
that it isn't worth bringing in a new dependency for it.
"""

from time import sleep
from datetime import datetime
from math import ceil
from random import randrange


class ExpBackoff:
    """
    ``SaturnCluster._start()`` requires polling until the
    Dask scheduled comes up. Exponential backoff is better
    in these situations than fixed-wait-time polling, because
    it minimizes the number of requests that need to be
    made from the beginning of polling to the time the
    scheduler is up.
    """

    def __init__(self, wait_timeout: int = 1200, min_sleep: int = 5, max_sleep: int = 60):
        """
        Used to generate sleep times with a capped exponential backoff.
        Jitter reduces contention on the event of multiple clients making
        these calls at the same time.

        :param wait_timeout: Maximum total time in seconds to wait before timing out
        :param min_sleep: Minimum amount of time to sleep in seconds
        :param max_sleep: Maximum time to sleep over one period in seconds
        :return: Boolean indicating if current wait time is less than wait_timeout
        """
        self.wait_timeout = wait_timeout
        self.max_sleep = max_sleep
        self.min_sleep = min_sleep
        self.retries = 0
        self.start_time = None

    def wait(self) -> bool:
        """
        This methods returns ``False`` if the timeout has been
        exceeded and code that is using ``ExpBackoff`` for polling
        should just consider the polling failed.

        If there there is still time left until
        ``self.wait_timeout``, waits for some time and then
        returns ``True``.
        """
        if self.retries == 0:
            self.start_time = datetime.now()

        # Check if timeout has been reached
        time_delta = (datetime.now() - self.start_time).total_seconds()
        if time_delta >= self.wait_timeout:
            return False

        # Generate exp backoff with jitter
        self.retries += 1
        backoff = min(self.max_sleep, self.min_sleep * 2 ** self.retries) / 2
        jitter = randrange(0, ceil(backoff))
        wait_time = backoff + jitter

        # Make sure we aren't waiting longer than wait_timeout
        remaining_time = self.wait_timeout - time_delta
        if remaining_time < wait_time:
            wait_time = remaining_time

        sleep(wait_time)
        return True
