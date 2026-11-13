"""Shared helpers for the example pipelines.

This module is not part of the library: it only keeps run metrics and CLI
options out of the example scripts, so each one can focus on its pipeline.
"""

from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

import argparse
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Sequence

# The examples are run from a source checkout, where the package is not installed:
# `python examples/foo.py` adds examples/ to sys.path, not the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from smartpipeline import Item  # noqa: E402

try:
    import resource
except ImportError:  # pragma: no cover - Windows
    resource = None  # type: ignore[assignment]


def add_scale_args(
    parser: argparse.ArgumentParser,
    items: int,
    item_label: str,
    workers: int = 4,
    queue_size: int = 1000,
) -> None:
    parser.add_argument(
        "--items",
        type=int,
        default=items,
        help=f"number of {item_label} to process (default: %(default)s)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=workers,
        help="concurrent workers of the main stage (default: %(default)s)",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=queue_size,
        help="max size of each queue between stages (default: %(default)s)",
    )


def configure_logging(level: int = logging.WARNING) -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        level=level,
    )


def _rss_mb(raw: int) -> float:
    # macOS reports ru_maxrss in bytes, Linux and the BSDs in KiB.
    return raw / (1024 * 1024) if sys.platform == "darwin" else raw / 1024


def peak_rss_mb() -> float:
    """Peak resident memory of this process."""
    if resource is None:
        return 0.0
    return _rss_mb(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def child_peak_rss_mb() -> float:
    """Peak resident memory of the largest child process seen so far."""
    if resource is None:
        return 0.0
    return _rss_mb(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)


class RunMetrics:
    """Collect delivered items, errors and per-stage timings from a run."""

    def __init__(self) -> None:
        self.items = 0
        self.soft_errors = 0
        self.critical_errors = 0
        self._stage_total: dict[str, float] = defaultdict(float)
        self._stage_count: dict[str, int] = defaultdict(int)
        self._stage_max: dict[str, float] = defaultdict(float)

    def add(self, item: Item[Any]) -> None:
        self.items += 1
        self.soft_errors += sum(1 for _ in item.soft_errors())
        self.critical_errors += sum(1 for _ in item.critical_errors())
        for name in item.timed_stages:
            seconds = item.get_timing(name) or 0.0
            self._stage_total[name] += seconds
            self._stage_count[name] += 1
            self._stage_max[name] = max(self._stage_max[name], seconds)

    def stage_rows(self) -> list[str]:
        rows = []
        for name, total in sorted(
            self._stage_total.items(), key=lambda entry: -entry[1]
        ):
            count = self._stage_count[name]
            rows.append(
                f"{name}: total={total:.2f}s "
                f"avg={1000 * total / count:.2f}ms "
                f"max={1000 * self._stage_max[name]:.1f}ms"
            )
        return rows

    def report(
        self,
        label: str,
        elapsed: float,
        extra: Sequence[str] = (),
    ) -> None:
        throughput = self.items / elapsed if elapsed else 0.0
        print(f"\n=== {label} ===")
        print(
            f"items={self.items} elapsed={elapsed:.2f}s "
            f"throughput={throughput:.1f} items/s"
        )
        print(
            f"soft_errors={self.soft_errors} " f"critical_errors={self.critical_errors}"
        )
        print(
            f"peak_rss={peak_rss_mb():.0f}MB "
            f"child_peak_rss={child_peak_rss_mb():.0f}MB"
        )
        for row in self.stage_rows():
            print(f"  {row}")
        for line in extra:
            print(f"  {line}")


def consume(pipeline: Any, metrics: RunMetrics) -> float:
    """Drain pipeline.run(), recording metrics, and return the elapsed time."""
    start = time.perf_counter()
    for item in pipeline.run():
        metrics.add(item)
    return time.perf_counter() - start
