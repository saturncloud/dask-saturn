from time import sleep
from datetime import datetime
from math import ceil
from random import randrange


class ExpBackoff:
    def __init__(self, wait_timeout=1200, min_sleep=5, max_sleep=60):
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
        jitter = randrange(0, ceil(backoff))
        wait_time = backoff + jitter

        # Make sure we aren't waiting longer than wait_timeout
        remaining_time = self.wait_timeout - time_delta
        if remaining_time < wait_time:
            wait_time = remaining_time

        sleep(wait_time)
        return True
