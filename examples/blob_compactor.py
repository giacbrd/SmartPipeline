"""Blob store compactor: a storage engineer's deduplication job.

Persona: a backend engineer compacting a directory of uploaded binary blobs,
deduplicating by content hash and producing an audit database.

Stresses:
- disk I/O plus zlib compression on threads (zlib releases the GIL)
- large items flowing between stages, with a deliberately small queue
- both thread and process executors over large pickled payloads
- unreadable/missing/empty files: critical and soft errors must not stop the run
- a batch SQLite writer whose connection crosses a thread boundary
- memory pressure: compressed payloads are kept in items until the sink

Run from the repository root::

    python examples/blob_compactor.py --items 240 --workers 4
    python examples/blob_compactor.py --items 60 --workers 2 --executor processes
"""

from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

import argparse
import hashlib
import os
import random
import shutil
import sqlite3
import tempfile
import time
import zlib
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
from smartpipeline.helpers import FilePathItem

TEXT_CHUNK = (
    b"2026-11-12T08:00:00Z INFO checkout order=ORD-12345 "
    b"user=u9876 amount=42.50 currency=EUR status=paid "
    b"trace=0123456789abcdef region=eu-west-1\n"
)


def make_corpus(root: Path, files: int, seed: int = 11) -> list[Path]:
    rng = random.Random(seed)
    paths: list[Path] = []
    for index in range(files):
        size = rng.randint(300_000, 900_000)
        if index % 5 == 0:
            # incompressible payloads keep zlib busy and make compression ratios real
            data = os.urandom(size)
        else:
            repeats = size // len(TEXT_CHUNK) + 1
            data = (TEXT_CHUNK * repeats)[:size]
        path = root / f"blob-{index:04d}.bin"
        path.write_bytes(data)
        paths.append(path)
        if index % 10 == 0:
            duplicate = root / f"blob-{index:04d}-copy.bin"
            shutil.copyfile(path, duplicate)
            paths.append(duplicate)
    for index in range(4):
        path = root / f"empty-{index}.bin"
        path.write_bytes(b"")
        paths.append(path)
    for index in range(2):
        path = root / f"unreadable-{index}.bin"
        path.write_bytes(b"secret")
        path.chmod(0o000)
        paths.append(path)
    # one large incompressible blob: a single item that dwarfs the queue
    huge = root / "huge-0000.bin"
    huge.write_bytes(os.urandom(16_000_000))
    paths.append(huge)
    return paths


class PathListSource(Source[Dict[str, Any]]):
    """Snapshot the file list, so files can vanish after the source is created."""

    def __init__(self, paths: Sequence[Path]):
        self.paths = list(paths)
        self._index = 0

    def pop(self):
        if self._index >= len(self.paths):
            self.stop()
            return None
        path = self.paths[self._index]
        self._index += 1
        return FilePathItem(str(path))


class BlobDigest(Stage[Dict[str, Any]]):
    """Read, hash and compress one blob."""

    def process(self, item: Item[Dict[str, Any]]) -> Item[Dict[str, Any]]:
        assert isinstance(item, FilePathItem)
        raw = Path(item.path).read_bytes()
        if not raw:
            raise SoftError("empty blob")
        packed = zlib.compress(raw, level=6)
        item.data.update(
            {
                "digest": hashlib.sha256(raw).hexdigest(),
                "raw_size": len(raw),
                "packed_size": len(packed),
                "packed": packed,
            }
        )
        return item


class BlobWriter(BatchStage[Dict[str, Any]]):
    """Deduplicate batches and write compressed blobs plus a SQLite manifest."""

    def __init__(
        self,
        out_dir: Path,
        db_path: Path,
        size: int = 32,
        timeout: float = 0.5,
        delay: float = 0.0,
    ):
        super().__init__(size, timeout)
        self._out_dir = out_dir
        self._db_path = db_path
        self._delay = delay
        self._connection: sqlite3.Connection | None = None
        self._seen: set[str] = set()
        self._rows = 0
        self._written = 0
        self._duplicates = 0
        self._skipped = 0
        self._raw_bytes = 0
        self._packed_bytes = 0

    def on_start(self) -> None:
        self._out_dir.mkdir(parents=True, exist_ok=True)
        # on_start runs on the main thread, process_batch on the single worker
        # thread: sqlite connections are thread-affine by default.
        self._connection = sqlite3.connect(self._db_path, check_same_thread=False)
        self._connection.execute(
            "CREATE TABLE blobs (name TEXT, digest TEXT, raw_size INTEGER, "
            "packed_size INTEGER, status TEXT, duplicate INTEGER)"
        )

    def process_batch(
        self, items: Sequence[Item[Dict[str, Any]]]
    ) -> Sequence[Item[Dict[str, Any]]]:
        assert self._connection is not None
        if self._delay:
            time.sleep(self._delay)  # simulate a slow manifest/database
        for item in items:
            self._rows += 1
            name = Path(item.path).name if isinstance(item, FilePathItem) else item.id
            digest = item.data.get("digest")
            if digest is None or "packed" not in item.data:
                status = "critical" if item.has_critical_errors() else "skipped"
                self._skipped += 1
                self._connection.execute(
                    "INSERT INTO blobs VALUES (?, ?, ?, ?, ?, ?)",
                    (name, None, 0, 0, status, 0),
                )
                continue
            duplicate = digest in self._seen
            if not duplicate:
                self._seen.add(digest)
                (self._out_dir / f"{digest}.z").write_bytes(item.data["packed"])
                self._written += 1
            else:
                self._duplicates += 1
            self._raw_bytes += item.data["raw_size"]
            self._packed_bytes += item.data["packed_size"]
            self._connection.execute(
                "INSERT INTO blobs VALUES (?, ?, ?, ?, ?, ?)",
                (
                    name,
                    digest,
                    item.data["raw_size"],
                    item.data["packed_size"],
                    "ok",
                    int(duplicate),
                ),
            )
        self._connection.commit()
        return items

    def on_end(self) -> None:
        if self._connection is not None:
            self._connection.commit()
            self._connection.close()
            self._connection = None


class Audit(Stage[Dict[str, Any]]):
    """Last inline stage, counting what actually made it through."""

    def __init__(self) -> None:
        self.items = 0
        self.failed = 0
        self.bytes = 0

    def process(self, item: Item[Dict[str, Any]]) -> Item[Dict[str, Any]]:
        self.items += 1
        if item.has_critical_errors() or item.has_soft_errors():
            self.failed += 1
        else:
            self.bytes += int(item.data.get("raw_size", 0))
        return item


class ExpectedErrors(ErrorManager):
    """Blob corpora contain bad files on purpose: suppress their expected tracebacks."""

    def handle(self, error, stage, item):
        if isinstance(error, SoftError):
            item.add_soft_error(stage.name, error)
            return None
        # do not log expected PermissionError/FileNotFoundError tracebacks
        item.add_critical_error(stage.name, error)
        return None


def build_pipeline(
    source: PathListSource,
    args: argparse.Namespace,
    out_dir: Path,
    db_path: Path,
    sink_delay: float,
) -> Pipeline[Dict[str, Any]]:
    return (
        Pipeline[Dict[str, Any]](max_queues_size=args.queue_size)
        .set_error_manager(ExpectedErrors())
        .set_source(source)
        .append(
            "digest",
            BlobDigest(),
            concurrency=max(1, args.workers),
            executor=args.executor,
        )
        .append(
            "store",
            BlobWriter(out_dir, db_path, size=32, delay=sink_delay),
            concurrency=1,
        )
        .append("audit", Audit())
        .build()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_scale_args(parser, items=240, item_label="blobs", workers=4, queue_size=16)
    parser.add_argument(
        "--executor",
        choices=("threads", "processes"),
        default="threads",
        help="executor of the read/compress stage (default: %(default)s)",
    )
    parser.add_argument(
        "--sink-delay",
        type=float,
        default=0.0,
        help="seconds per batch spent in the slow manifest writer (default: %(default)s)",
    )
    args = parser.parse_args()
    configure_logging()

    with tempfile.TemporaryDirectory(prefix="smartpipeline-blobs-") as tmp:
        root = Path(tmp)
        source_dir = root / "incoming"
        out_dir = root / "compacted"
        source_dir.mkdir()
        started = time.perf_counter()
        paths = make_corpus(source_dir, args.items)
        generation = time.perf_counter() - started
        source = PathListSource(paths)
        # simulate files disappearing while the pipeline is being configured
        paths[3].unlink(missing_ok=True)
        paths[7].unlink(missing_ok=True)

        pipeline = build_pipeline(
            source,
            args,
            out_dir,
            root / "manifest.db",
            args.sink_delay,
        )
        metrics = RunMetrics()
        elapsed = consume(pipeline, metrics)

        writer = pipeline.get_stage("store")
        audit = pipeline.get_stage("audit")
        assert isinstance(writer, BlobWriter)
        assert isinstance(audit, Audit)
        ratio = writer._raw_bytes / max(1, writer._packed_bytes)
        metrics.report(
            "Blob compactor",
            elapsed,
            extra=[
                f"executor={args.executor} workers={args.workers} "
                f"queue_size={args.queue_size} sink_delay={args.sink_delay}",
                f"corpus_files={len(paths)} generated_in={generation:.2f}s",
                f"written={writer._written} duplicates={writer._duplicates} "
                f"skipped={writer._skipped} rows={writer._rows}",
                f"raw={writer._raw_bytes / 1e6:.1f}MB "
                f"packed={writer._packed_bytes / 1e6:.1f}MB ratio={ratio:.2f}x",
                f"audit_items={audit.items} audit_failed={audit.failed} "
                f"audit_ok_bytes={audit.bytes / 1e6:.1f}MB",
                f"pipeline_error_totals="
                f"soft:{pipeline.soft_errors_count} "
                f"critical:{pipeline.critical_errors_count}",
                f"manifest_rows={writer._rows}",
            ],
        )


if __name__ == "__main__":
    main()
