
from datetime import datetime


class ProgressPrinter:
    size: int
    steps: int
    i: int
    start_time: datetime

    def __init__(self, size: int, steps: int = 10):
        self.size = size
        self.steps = 10
        self.i = 0
        self.start_time = datetime.now()

    def start(self) -> None:
        self.start_time = datetime.now()

    def step(self, message: str) -> None:
        self.i += 1
        if self.i % self.steps == 0:
            remain_time: str = str((datetime.now() - self.start_time) * (self.size-self.i) / self.i).split('.')[0]
            print(
                f"{self.i} / {self.size} ({100*self.i/self.size:.1f}%) Remains: {remain_time}  ->  {message}",
                " " * 20,
                end='\r'
            )
