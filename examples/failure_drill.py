"""Failure drills: a release engineer's game day for pipeline robustness.

Persona: an SRE validating SmartPipeline's failure and lifecycle contracts
before a release, with poison items, broken batch contracts and cancellation.

Drills:
1. a batch stage returning the wrong number of items must fail the run with
   ``BatchOutputError`` (never retry) and still close the pipeline cleanly
2. a critical error skips the item downstream but ``run()`` still delivers it;
   ``raise_on_critical_error()`` promotes it to a run-level failure
3. closing from another thread while workers are busy must terminate without
   hanging and run ``on_end`` exactly once
4. a queue of size 1 must not deadlock a multi-stage pipeline

Run from the repository root::

    python examples/failure_drill.py
"""

from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

import logging
import threading
import time
from typing import List

from _common import configure_logging

from smartpipeline import (
    BatchStage,
    CriticalError,
    ErrorManager,
    Item,
    Pipeline,
    SoftError,
    Source,
    Stage,
)
from smartpipeline.error.exceptions import BatchOutputError
from smartpipeline.pipeline import PipelineState


class ItemsSource(Source[dict]):
    def __init__(self, count: int):
        self._count = count
        self._index = 0

    def pop(self):
        if self._index >= self._count:
            self.stop()
            return None
        self._index += 1
        return Item[dict]({"i": self._index})


class BrokenBatch(BatchStage[dict]):
    def __init__(self):
        super().__init__(size=4, timeout=0.05)
        self.started = 0
        self.ended = 0

    def on_start(self) -> None:
        self.started += 1

    def on_end(self) -> None:
        self.ended += 1

    def process_batch(self, items):
        # violates the one-result-per-input contract on purpose
        return list(items)[:-1]


class CriticalOnTwo(Stage[dict]):
    def process(self, item: Item[dict]) -> Item[dict]:
        if item.data["i"] == 2:
            raise CriticalError("poison item")
        if item.data["i"] == 3:
            raise SoftError("soft item")
        return item


class Seen(Stage[dict]):
    def __init__(self) -> None:
        self.items = 0

    def process(self, item: Item[dict]) -> Item[dict]:
        self.items += 1
        return item


class SlowStage(Stage[dict]):
    def __init__(self) -> None:
        self.started = 0
        self.ended = 0

    def on_start(self) -> None:
        self.started += 1

    def on_end(self) -> None:
        self.ended += 1

    def process(self, item: Item[dict]) -> Item[dict]:
        time.sleep(0.02)
        return item


def drill_batch_contract() -> tuple[bool, str]:
    stage = BrokenBatch()
    pipeline = (
        Pipeline[dict]()
        .set_source(ItemsSource(20))
        .append("broken", stage)
        .append("seen", Seen())
        .build()
    )
    try:
        list(pipeline.run())
    except BatchOutputError:
        ok = (
            pipeline.state is PipelineState.CLOSED
            and stage.started == 1
            and stage.ended == 1
        )
        return ok, (
            f"raised BatchOutputError, state={pipeline.state.value}, "
            f"started={stage.started}, ended={stage.ended}"
        )
    except BaseException as error:  # pragma: no cover - diagnostic
        return False, f"unexpected {type(error).__name__}: {error}"
    return False, "the run did not raise"


def drill_critical_errors() -> tuple[bool, str]:
    # default manager: skip the item downstream, deliver it anyway
    seen = Seen()
    pipeline = (
        Pipeline[dict]()
        .set_source(ItemsSource(5))
        .append("critical", CriticalOnTwo())
        .append("seen", seen)
        .build()
    )
    delivered = list(pipeline.run())
    with_critical = sum(1 for item in delivered if item.has_critical_errors())
    with_soft = sum(1 for item in delivered if item.has_soft_errors())
    first = (
        len(delivered) == 5
        and seen.items == 4
        and with_critical == 1
        and with_soft == 1
    )

    # raise_on_critical_error: the original exception reaches the caller
    pipeline = (
        Pipeline[dict]()
        .set_error_manager(ErrorManager().raise_on_critical_error())
        .set_source(ItemsSource(5))
        .append("critical", CriticalOnTwo())
        .build()
    )
    raised = None
    try:
        list(pipeline.run())
    except CriticalError as error:
        raised = error
    second = raised is not None and "poison item" in str(raised)
    return (
        first and second,
        f"delivered={len(delivered)} downstream={seen.items} "
        f"critical={with_critical} soft={with_soft} raised={raised is not None}",
    )


def drill_cancel() -> tuple[bool, str]:
    stage = SlowStage()
    pipeline = (
        Pipeline[dict](max_queues_size=10)
        .set_source(ItemsSource(500))
        .append("slow", stage, concurrency=4)
        .append("tail", SlowStage())
        .build()
    )
    delivered: List[Item[dict]] = []
    errors: List[BaseException] = []

    def consume() -> None:
        try:
            for item in pipeline.run():
                delivered.append(item)
        except BaseException as error:  # pragma: no cover - diagnostic
            errors.append(error)

    thread = threading.Thread(target=consume)
    thread.start()
    time.sleep(0.25)
    started = time.perf_counter()
    pipeline.close()
    close_time = time.perf_counter() - started
    thread.join(timeout=5)
    return (
        not thread.is_alive()
        and not errors
        and pipeline.state is PipelineState.CLOSED
        and stage.started == 1
        and stage.ended == 1
        and close_time < 5,
        f"delivered={len(delivered)} close={close_time:.3f}s "
        f"started={stage.started} ended={stage.ended} errors={errors}",
    )


def drill_tiny_queue() -> tuple[bool, str]:
    pipeline = (
        Pipeline[dict](max_queues_size=1)
        .set_source(ItemsSource(200))
        .append("fast", Seen(), concurrency=2)
        .append("slow", SlowStage(), concurrency=3)
        .append("tail", Seen())
        .build()
    )
    started = time.perf_counter()
    delivered = sum(1 for _ in pipeline.run())
    elapsed = time.perf_counter() - started
    return (
        delivered == 200 and pipeline.state is PipelineState.CLOSED,
        f"delivered={delivered} elapsed={elapsed:.2f}s",
    )


def main() -> None:
    # drills intentionally trigger logged critical errors: keep the output readable
    configure_logging(logging.CRITICAL)
    drills = (
        ("batch contract", drill_batch_contract),
        ("critical errors", drill_critical_errors),
        ("cancel in flight", drill_cancel),
        ("queue size 1", drill_tiny_queue),
    )
    failures = 0
    for name, drill in drills:
        started = time.perf_counter()
        try:
            ok, detail = drill()
        except BaseException as error:  # pragma: no cover - diagnostic
            ok, detail = False, f"{type(error).__name__}: {error}"
        elapsed = time.perf_counter() - started
        print(f"[{'PASS' if ok else 'FAIL'}] {name} ({elapsed:.2f}s): {detail}")
        failures += not ok
    if failures:
        raise SystemExit(f"{failures} drill(s) failed")
    print("all drills passed")


if __name__ == "__main__":
    main()
