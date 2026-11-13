"""Log lake ETL: an SRE team's access-log analytics job.

Persona: a platform engineer turning a big nginx-style log file into
per-minute/service error statistics.

Stresses:
- a fast inline parser feeding a deliberately CPU-heavy stage on processes
- a bounded queue that has to absorb a large speed mismatch
- malformed records handled as soft errors, without poisoning the run
- a batch aggregator with per-worker state and a final inline report
- process startup, pickling overhead and child-process memory

Run from the repository root::

    python examples/log_lake_etl.py --items 60000 --workers 4
    python examples/log_lake_etl.py --items 60000 --executor threads
"""

from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

import argparse
import json
import random
import re
import tempfile
import time
import zlib
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Sequence

from _common import RunMetrics, add_scale_args, configure_logging, consume

from smartpipeline import (
    BatchStage,
    ErrorManager,
    Item,
    Pipeline,
    SoftError,
    Source,
    Stage,
)

LOG_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ) "
    r"(?P<level>[A-Z]+) "
    r"(?P<service>[a-z]+) "
    r"latency=(?P<latency>\d+) "
    r"status=(?P<status>\d+) "
    r'msg="(?P<message>.*)"$'
)
WORD_RE = re.compile(r"[a-z0-9]{3,}")
ERROR_TERMS = frozenset({"error", "timeout", "refused", "failed", "panic", "retry"})
SIGNATURE_DIM = 256
# A fixed linear model: 16 prototypes of 128 features, evaluated per item.
PROTOTYPES = tuple(
    tuple(((row * 31 + col * 17) % 13) - 6 for col in range(SIGNATURE_DIM))
    for row in range(32)
)
SERVICES = ("checkout", "auth", "search", "payments", "inventory")
PATHS = (
    "GET /api/v1/orders",
    "POST /api/v1/orders",
    "GET /api/v1/users",
    "POST /api/v1/payments",
    "GET /health",
    "GET /api/v1/search",
)


def generate_log_file(path: Path, lines: int, seed: int = 7) -> None:
    rng = random.Random(seed)
    start = datetime(2026, 11, 12, 8, 0, 0)
    with path.open("w", encoding="utf-8") as output:
        for index in range(lines):
            if rng.random() < 0.002:
                # deliberately broken lines: the parser turns them into soft errors
                output.write(
                    f'this line is not a log record #{index} "{rng.random()}"\n'
                )
                continue
            timestamp = start + timedelta(seconds=index // 20)
            roll = rng.random()
            if roll < 0.86:
                level, status = "INFO", rng.choice((200, 201, 204))
            elif roll < 0.94:
                level, status = "WARN", rng.choice((400, 401, 404, 429))
            elif roll < 0.995:
                level, status = "ERROR", rng.choice((500, 502, 503))
            else:
                level, status = "DEBUG", 200
            latency = max(1, int(rng.lognormvariate(4.3, 0.8)))
            message = (
                f"{rng.choice(PATHS)} user=u{rng.randrange(10000)} "
                f"cache={rng.choice(('hit', 'miss'))} "
                f"region={rng.choice(('eu', 'us', 'apac'))} "
                f"trace={rng.getrandbits(48):012x}"
            )
            output.write(
                f"{timestamp:%Y-%m-%dT%H:%M:%SZ} {level} "
                f"{rng.choice(SERVICES)} latency={latency} status={status} "
                f'msg="{message}"\n'
            )


class LogLineSource(Source[Dict[str, Any]]):
    def __init__(self, path: Path):
        self._file = path.open(encoding="utf-8")
        self._index = 0

    def pop(self):
        line = self._file.readline()
        if not line:
            self._file.close()
            self.stop()
            return None
        self._index += 1
        return Item[Dict[str, Any]]({"line": line.rstrip("\n"), "line_no": self._index})


class QuietErrorManager(ErrorManager):
    """Count expected parse errors instead of logging thousands of tracebacks."""

    def __init__(self) -> None:
        self.soft = 0
        self.critical = 0

    def handle(self, error, stage, item):
        if isinstance(error, SoftError):
            item.add_soft_error(stage.name, error)
            self.soft += 1
            return None
        self.critical += 1
        return super().handle(error, stage, item)


class ParseLog(Stage[Dict[str, Any]]):
    """Fast inline parsing; malformed lines are expected and become soft errors."""

    def process(self, item: Item[Dict[str, Any]]) -> Item[Dict[str, Any]]:
        match = LOG_RE.match(item.data["line"])
        if match is None:
            raise SoftError("malformed access-log line")
        data = item.data
        data.update(match.groupdict())
        data["latency"] = int(data["latency"])
        data["status"] = int(data["status"])
        data["minute"] = data["timestamp"][:16]
        # the raw line is the biggest field and is not needed downstream
        del data["line"]
        return item


class EnrichSignals(BatchStage[Dict[str, Any]]):
    """CPU-heavy tokenization and feature hashing, batched to amortize IPC."""

    def __init__(self, size: int = 256, timeout: float = 0.5) -> None:
        super().__init__(size, timeout)
        self._tokens = WORD_RE

    def _enrich(self, item: Item[Dict[str, Any]]) -> Item[Dict[str, Any]]:
        if item.has_soft_errors():
            return item
        message = item.data["message"]
        tokens = self._tokens.findall(message.lower())
        signature = [0] * SIGNATURE_DIM
        shingles = set()
        for token in tokens:
            value = zlib.crc32(token.encode())
            signature[value % SIGNATURE_DIM] += 1 if value & 1 else -1
        for start in range(max(0, len(tokens) - 2)):
            end = start + 3
            shingle = " ".join(tokens[start:end])
            shingles.add(zlib.crc32(shingle.encode()))
        item.data["tokens"] = len(tokens)
        item.data["error_terms"] = sum(1 for t in tokens if t in ERROR_TERMS)
        scores = [
            sum(feature * weight for feature, weight in zip(signature, prototype))
            for prototype in PROTOTYPES
        ]
        item.data["anomaly_score"] = round(max(scores) / 1000.0, 3)
        item.data["category"] = scores.index(max(scores))
        item.data["fingerprint"] = zlib.crc32(message.encode())
        item.data["shingle_count"] = len(shingles)
        return item

    def process_batch(
        self, items: Sequence[Item[Dict[str, Any]]]
    ) -> Sequence[Item[Dict[str, Any]]]:
        return [self._enrich(item) for item in items]


class WindowAggregator(BatchStage[Dict[str, Any]]):
    """Aggregate per minute/service/level with one worker and write a report."""

    def __init__(self, path: Path, size: int = 4000, timeout: float = 1.0):
        super().__init__(size, timeout)
        self._path = path
        self._buckets: Counter[tuple[str, str, str]] = Counter()
        self._latency: Dict[tuple[str, str, str], list[int]] = defaultdict(
            lambda: [0, 0]
        )
        self._services: Counter[str] = Counter()
        self._items = 0
        self._skipped = 0

    def process_batch(
        self, items: Sequence[Item[Dict[str, Any]]]
    ) -> Sequence[Item[Dict[str, Any]]]:
        for item in items:
            if item.has_soft_errors() or item.has_critical_errors():
                self._skipped += 1
                continue
            data = item.data
            key = (data["minute"], data["service"], data["level"])
            self._buckets[key] += 1
            stats = self._latency[key]
            stats[0] += data["latency"]
            stats[1] = max(stats[1], data["latency"])
            self._services[data["service"]] += 1
            self._items += 1
        return items

    def on_end(self) -> None:
        rows = []
        for key, count in self._buckets.most_common(50):
            total, maximum = self._latency[key]
            rows.append(
                {
                    "minute": key[0],
                    "service": key[1],
                    "level": key[2],
                    "count": count,
                    "avg_latency": round(total / count, 2),
                    "max_latency": maximum,
                }
            )
        self._path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


class FinalReport(Stage[Dict[str, Any]]):
    """Last inline stage: runs on the caller's thread and sees every result."""

    def __init__(self) -> None:
        self.items = 0
        self.slow = 0
        self.server_errors = 0
        self.malformed = 0

    def process(self, item: Item[Dict[str, Any]]) -> Item[Dict[str, Any]]:
        self.items += 1
        if item.has_soft_errors():
            self.malformed += 1
            return item
        if item.data["latency"] >= 1000:
            self.slow += 1
        if item.data["status"] >= 500:
            self.server_errors += 1
        return item


def build_pipeline(
    path: Path,
    args: argparse.Namespace,
    report_path: Path,
    error_manager: QuietErrorManager,
) -> Pipeline[Dict[str, Any]]:
    return (
        Pipeline[Dict[str, Any]](max_queues_size=args.queue_size)
        .set_error_manager(error_manager)
        .set_source(LogLineSource(path))
        .append("parse", ParseLog())
        .append(
            "enrich",
            EnrichSignals(size=256),
            concurrency=max(1, args.workers),
            executor=args.executor,
        )
        .append("aggregate", WindowAggregator(report_path), concurrency=1)
        .append("report", FinalReport())
        .build()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_scale_args(parser, items=60_000, item_label="log lines", workers=4)
    parser.add_argument(
        "--executor",
        choices=("threads", "processes"),
        default="processes",
        help="executor of the CPU-heavy stage (default: %(default)s)",
    )
    args = parser.parse_args()
    configure_logging()

    with tempfile.TemporaryDirectory(prefix="smartpipeline-logs-") as tmp:
        root = Path(tmp)
        log_path = root / "access.log"
        report_path = root / "windows.json"
        started = time.perf_counter()
        generate_log_file(log_path, args.items)
        generation = time.perf_counter() - started

        error_manager = QuietErrorManager()
        pipeline = build_pipeline(log_path, args, report_path, error_manager)
        metrics = RunMetrics()
        elapsed = consume(pipeline, metrics)

        aggregator = pipeline.get_stage("aggregate")
        report = pipeline.get_stage("report")
        assert isinstance(aggregator, WindowAggregator)
        assert isinstance(report, FinalReport)
        metrics.report(
            "Access-log ETL",
            elapsed,
            extra=[
                f"executor={args.executor} workers={args.workers} "
                f"queue_size={args.queue_size}",
                f"log_file={log_path.stat().st_size / 1e6:.1f}MB "
                f"generated_in={generation:.2f}s",
                f"parsed={aggregator._items} skipped={aggregator._skipped} "
                f"parse_soft_errors={error_manager.soft} "
                f"parse_critical={error_manager.critical}",
                f"slow_requests={report.slow} server_errors={report.server_errors}",
                f"services={dict(aggregator._services.most_common(3))}",
                f"windows_report={report_path.stat().st_size if report_path.exists() else 0}B",
            ],
        )


if __name__ == "__main__":
    main()
