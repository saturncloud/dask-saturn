from distributed.comm import resolve_address


class SaturnSetup:
    name = "saturn_setup"

    def setup(self, worker=None):
        worker.scheduler.addr = resolve_address(worker.scheduler.addr)
