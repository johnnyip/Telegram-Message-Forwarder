from .utils import now_ts


class StepTimer:
    def __init__(self):
        self.start = now_ts()

    def ms(self) -> int:
        return int((now_ts() - self.start) * 1000)
