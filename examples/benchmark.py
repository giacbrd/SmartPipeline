__author__ = "Giacomo Berardi <giacbrd.com>"

"""Baseline benchmark for SmartPipeline.

Times 20k items through four pipelines: inline, one threads stage with
concurrency 2, one processes stage with concurrency 2, and a batch stage of
size 100. It also reports the thread/process break-even for 0.1ms and 1ms of
pure-Python CPU work per item, where the queue transport cost is visible.
Plain script, not a test.
"""

import time

import _common  # noqa: F401  (puts the repository root on sys.path)

from smartpipeline.item import Item
from smartpipeline.pipeline import Pipeline
from smartpipeline.stage import BatchStage, Source, Stage


class TextSource(Source):
    def __init__(self, count):
        self.total = count
        self.counter = 0

    def pop(self):
        self.counter += 1
        if self.counter > self.total:
            self.stop()
            return None
        return Item({"text": "hello world", "count": self.counter})


class Upper(Stage):
    def process(self, item: Item):
        item.data["text"] = item.data["text"].upper()
        return item


class CpuBurn(Stage):
    """Busy-wait CPU work, so threads are limited by the GIL."""

    def __init__(self, seconds):
        self._seconds = seconds

    def process(self, item: Item):
        deadline = time.perf_counter() + self._seconds
        iterations = 0
        while time.perf_counter() < deadline:
            iterations += 1
        item.data["iterations"] = iterations
        return item


class UpperBatch(BatchStage):
    def __init__(self, size=100, timeout=1.0):
        super().__init__(size, timeout)

    def process_batch(self, items):
        for item in items:
            item.data["text"] = item.data["text"].upper()
        return items


def run_case(name, pipeline):
    start = time.perf_counter()
    count = 0
    for _ in pipeline.run():
        count += 1
    elapsed = time.perf_counter() - start
    print(f"{name}: {count} items in {elapsed:.2f}s ({count / elapsed:.0f} items/s)")
    return elapsed


def main():
    total = 20000
    run_case(
        "inline",
        Pipeline().set_source(TextSource(total)).append("upper", Upper()),
    )
    run_case(
        "threads-2",
        Pipeline()
        .set_source(TextSource(total))
        .append("upper", Upper(), concurrency=2, executor="threads"),
    )
    run_case(
        "processes-2",
        Pipeline()
        .set_source(TextSource(total))
        .append("upper", Upper(), concurrency=2, executor="processes"),
    )
    run_case(
        "batch-100",
        Pipeline()
        .set_source(TextSource(total))
        .append("upper", UpperBatch(size=100), concurrency=2, executor="threads"),
    )
    for work, count in (("0.1ms", 5000), ("1ms", 2000)):
        seconds = 0.0001 if work == "0.1ms" else 0.001
        for executor in ("threads", "processes"):
            run_case(
                f"cpu-{work} {executor}-2",
                Pipeline()
                .set_source(TextSource(count))
                .append(
                    "burn",
                    CpuBurn(seconds),
                    concurrency=2,
                    executor=executor,
                ),
            )


if __name__ == "__main__":
    main()
