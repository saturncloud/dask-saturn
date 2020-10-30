from distributed.comm import resolve_address


class SaturnSetup:
    name = "saturn_setup"

    def __init__(self, scheduler_address=None):
        self.scheduler_address = scheduler_address

    def setup(self, worker=None):
        worker.scheduler.addr = resolve_address(self.scheduler_address)
