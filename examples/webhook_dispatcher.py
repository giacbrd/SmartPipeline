"""Webhook dispatcher: a SaaS notification delivery pipeline.

Persona: a platform engineer sending subscription events to customer webhooks,
with retries, a dead-letter queue and an audit trail.

Stresses:
- a local ThreadingHTTPServer acting as a flaky third-party endpoint
- retries with backoff on 5xx responses, timeouts and connection errors
- per-thread ``requests.Session`` objects on a concurrent stage
- soft errors that must reach a downstream dead-letter stage
- a fast producer feeding slow HTTP delivery and a batch audit sink

Run from the repository root::

    python examples/webhook_dispatcher.py --items 1500 --workers 8
"""

from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

import argparse
import json
import tempfile
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Sequence, TypedDict

import requests
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
from smartpipeline.error.exceptions import RetryError


class EventData(TypedDict):
    event_id: str
    kind: str
    customer: str
    text: str
    payload: Dict[str, Any]
    delivered: bool
    status: int
    attempts: int
    dlq_reason: str


class TransientDeliveryError(Exception):
    """A 429/5xx response from the webhook endpoint."""


class FlakyHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address):
        super().__init__(address, FlakyHandler)
        self.attempts: Dict[str, int] = defaultdict(int)
        self.lock = threading.Lock()


class FlakyHandler(BaseHTTPRequestHandler):
    """Fails deterministically based on the event id in X-Event-Id."""

    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        event_id = self.headers.get("X-Event-Id", "")
        try:
            suffix = int(event_id.rsplit("-", 1)[-1])
        except ValueError:
            suffix = 0
        server = self.server
        assert isinstance(server, FlakyHTTPServer)
        with server.lock:
            server.attempts[event_id] += 1
            attempt = server.attempts[event_id]
        if suffix % 97 == 0:
            status = 500  # poison event: every attempt fails
        elif suffix % 7 == 0 and attempt <= 2:
            status = 500  # flaky event: succeeds on the third attempt
        elif suffix % 11 == 0:
            time.sleep(0.05)  # slow endpoint
            status = 200
        else:
            status = 200
        body = json.dumps({"ok": status == 200}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


class EventSource(Source[EventData]):
    def __init__(self, total: int):
        self._total = total
        self._index = 0

    def pop(self):
        if self._index >= self._total:
            self.stop()
            return None
        self._index += 1
        index = self._index
        return Item[EventData](
            {
                "event_id": f"evt-{index:06d}",
                "kind": ("invoice.paid", "invoice.failed", "plan.changed")[index % 3],
                "customer": f"cus-{index % 500:04d}",
                "text": (
                    f"Subscription event {index} for customer {index % 500}; "
                    "please update your records and acknowledge delivery."
                ),
                "payload": {},
                "delivered": False,
                "status": 0,
                "attempts": 0,
                "dlq_reason": "",
            }
        )


class Render(Stage[EventData]):
    """Cheap inline stage: build the webhook body."""

    def process(self, item: Item[EventData]) -> Item[EventData]:
        data = item.data
        data["payload"] = {
            "id": data["event_id"],
            "type": data["kind"],
            "customer": data["customer"],
            "text": data["text"],
            "timestamp": "2026-11-12T08:00:00Z",
        }
        return item


class Deliver(Stage[EventData]):
    """POST the payload; transient failures are retried by the pipeline."""

    def __init__(self, url: str, timeout: float = 2.0):
        self._url = url
        self._timeout = timeout
        self._local = threading.local()
        self._sessions: list[requests.Session] = []
        self._lock = threading.Lock()

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({"User-Agent": "SmartPipeline-webhook/1.0"})
            self._local.session = session
            with self._lock:
                self._sessions.append(session)
        return session

    def process(self, item: Item[EventData]) -> Item[EventData]:
        data = item.data
        data["attempts"] += 1
        response = self._session().post(
            self._url,
            json=data["payload"],
            headers={"X-Event-Id": data["event_id"]},
            timeout=self._timeout,
        )
        if response.status_code in (429, 500, 502, 503, 504):
            raise TransientDeliveryError(
                f"HTTP {response.status_code} delivering {data['event_id']}"
            )
        response.raise_for_status()
        data["delivered"] = True
        data["status"] = response.status_code
        return item

    def on_end(self) -> None:
        with self._lock:
            for session in self._sessions:
                session.close()
            self._sessions.clear()


class DeadLetter(Stage[EventData]):
    """Inline stage after delivery: mark exhausted retries for the DLQ."""

    def process(self, item: Item[EventData]) -> Item[EventData]:
        if item.has_soft_errors():
            for error in item.soft_errors():
                if isinstance(error, RetryError):
                    item.data["dlq_reason"] = str(error)
                    break
        return item


class Audit(BatchStage[EventData]):
    def __init__(self, path: Path, size: int = 50, timeout: float = 0.5):
        super().__init__(size, timeout)
        self._path = path
        self._file = None
        self.rows = 0
        self.delivered = 0
        self.dead_letters = 0
        self.attempts = 0

    def on_start(self) -> None:
        self._file = self._path.open("w", encoding="utf-8")

    def process_batch(
        self, items: Sequence[Item[EventData]]
    ) -> Sequence[Item[EventData]]:
        assert self._file is not None
        for item in items:
            data = item.data
            self.rows += 1
            self.attempts += data["attempts"]
            if data["delivered"]:
                self.delivered += 1
            if data["dlq_reason"]:
                self.dead_letters += 1
            self._file.write(
                json.dumps(
                    {
                        "event_id": data["event_id"],
                        "delivered": data["delivered"],
                        "status": data["status"],
                        "attempts": data["attempts"],
                        "dlq_reason": data["dlq_reason"],
                    }
                )
                + "\n"
            )
        self._file.flush()
        return items

    def on_end(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


class QuietErrors(ErrorManager):
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


def build_pipeline(
    source: EventSource,
    url: str,
    args: argparse.Namespace,
    audit_path: Path,
    error_manager: QuietErrors,
) -> Pipeline[EventData]:
    return (
        Pipeline[EventData](max_queues_size=args.queue_size)
        .set_error_manager(error_manager)
        .set_source(source)
        .append("render", Render())
        .append(
            "deliver",
            Deliver(url),
            concurrency=max(1, args.workers),
            retryable_errors=(
                TransientDeliveryError,
                requests.RequestException,
            ),
            max_retries=2,
            backoff=0.05,
        )
        .append("dead_letter", DeadLetter())
        .append("audit", Audit(audit_path), concurrency=1)
        .build()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_scale_args(
        parser, items=1500, item_label="webhook events", workers=8, queue_size=128
    )
    args = parser.parse_args()
    configure_logging()

    server = FlakyHTTPServer(("127.0.0.1", 0))
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    url = f"http://127.0.0.1:{server.server_port}/deliver"
    try:
        with tempfile.TemporaryDirectory(prefix="smartpipeline-hooks-") as tmp:
            audit_path = Path(tmp) / "audit.jsonl"
            source = EventSource(args.items)
            error_manager = QuietErrors()
            pipeline = build_pipeline(source, url, args, audit_path, error_manager)
            metrics = RunMetrics()
            elapsed = consume(pipeline, metrics)

            audit = pipeline.get_stage("audit")
            assert isinstance(audit, Audit)
            metrics.report(
                "Webhook dispatcher",
                elapsed,
                extra=[
                    f"workers={args.workers} queue_size={args.queue_size} "
                    f"url={url}",
                    f"delivered={audit.delivered} "
                    f"dead_letters={audit.dead_letters} rows={audit.rows}",
                    f"item_attempts={audit.attempts} "
                    f"server_requests={sum(server.attempts.values())}",
                    f"soft_errors={error_manager.soft} "
                    f"critical_errors={error_manager.critical}",
                    f"audit_bytes={audit_path.stat().st_size if audit_path.exists() else 0}",
                ],
            )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)


if __name__ == "__main__":
    main()
