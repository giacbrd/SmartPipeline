"""Fraud scoring stream: a payments team's async risk pipeline.

Persona: a fintech ML engineer scoring card transactions as they arrive,
with a flaky merchant-enrichment gateway and a batch risk model.

Stresses:
- ``process_async()``/``get_item()`` instead of a source, with a bounded
  in-flight window (producer backpressure)
- retries with exponential backoff on a simulated flaky external gateway
- a thread stage feeding a two-worker batch model, then a last inline stage
- per-item callbacks, metadata carried across stages, and shutdown
- latency and retry accounting from delivered items

Run from the repository root::

    python examples/fraud_stream.py --items 6000 --workers 4
"""

from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

import argparse
import math
import random
import threading
import time
from typing import List, Sequence, TypedDict

from _common import RunMetrics, add_scale_args, configure_logging

from smartpipeline import BatchStage, Item, Pipeline, SoftError, Stage
from smartpipeline.pipeline import PipelineStateError

COUNTRIES = ("IT", "DE", "FR", "US", "BR", "NG", "RU")
COUNTRY_RISK = {
    "IT": 0.1,
    "DE": 0.1,
    "FR": 0.1,
    "US": 0.2,
    "BR": 0.4,
    "NG": 0.7,
    "RU": 0.8,
}
DEVICES = ("ios", "android", "web", "pos")
DEVICE_RISK = {"ios": 0.1, "android": 0.2, "web": 0.45, "pos": 0.3}
# A deliberately tiny "model": intercept plus 8 features.
WEIGHTS = (-2.5, 2.4, -1.1, 1.8, 1.2, 0.9, 1.5, -0.7, 0.4)


class TransactionData(TypedDict):
    txn_id: str
    amount: float
    currency: str
    merchant: str
    country: str
    device: str
    hour: int
    merchant_risk: float
    velocity: float
    score: float
    decision: str
    latency_ms: float
    features: list[float]


class GatewayTimeout(Exception):
    """Retryable failure of the merchant enrichment service."""


class Validate(Stage[TransactionData]):
    def process(self, item: Item[TransactionData]) -> Item[TransactionData]:
        data = item.data
        if data["amount"] <= 0:
            raise SoftError("non-positive transaction amount")
        data["txn_id"] = str(data["txn_id"])
        return item


class MerchantEnrichment(Stage[TransactionData]):
    """Simulated external call: slow, occasionally transient, one retry succeeds."""

    def __init__(self) -> None:
        self.calls = 0
        self.timeouts = 0
        self._lock = threading.Lock()

    def process(self, item: Item[TransactionData]) -> Item[TransactionData]:
        data = item.data
        attempt = int(item.metadata.get("enrich_attempts", 0)) + 1
        item.metadata["enrich_attempts"] = attempt
        with self._lock:
            self.calls += 1
        suffix = int(data["txn_id"].rsplit("-", 1)[-1])
        # deterministic latency: most calls are fast, a few are slow
        time.sleep(0.002 + (suffix % 5) * 0.001)
        if suffix % 101 == 0:
            time.sleep(0.03)
        # poison a few calls on the first attempt only, so retries can recover
        if suffix % 23 == 0 and attempt == 1:
            with self._lock:
                self.timeouts += 1
            raise GatewayTimeout(f"merchant timeout for {data['txn_id']}")
        data["merchant_risk"] = round(0.1 + (suffix % 17) / 20.0, 3)
        data["velocity"] = round(min(1.0, (suffix % 31) / 30.0), 3)
        return item


def _features(data: TransactionData) -> list[float]:
    return [
        1.0,
        min(data["amount"], 5000.0) / 5000.0,
        data["hour"] / 24.0,
        COUNTRY_RISK.get(data["country"], 0.5),
        DEVICE_RISK.get(data["device"], 0.5),
        data.get("merchant_risk", 0.5),
        data.get("velocity", 0.2),
        (data["amount"] > 1000.0) * 1.0,
        data["currency"] != "EUR" and 1.0 or 0.0,
    ]


class RiskModel(BatchStage[TransactionData]):
    """A batch logistic model; sleeping stands in for a real model call."""

    def __init__(self, size: int = 64, timeout: float = 0.05):
        super().__init__(size, timeout)
        self.batches = 0
        self.samples = 0
        self._lock = threading.Lock()

    def process_batch(
        self, items: Sequence[Item[TransactionData]]
    ) -> Sequence[Item[TransactionData]]:
        with self._lock:
            self.batches += 1
        time.sleep(0.004 + len(items) * 0.0001)
        for item in items:
            if item.has_critical_errors():
                continue
            features = _features(item.data)
            z = sum(weight * value for weight, value in zip(WEIGHTS, features))
            item.data["features"] = features
            item.data["score"] = round(1.0 / (1.0 + math.exp(-z)), 4)
        with self._lock:
            self.samples += len(items)
        return items


class Decide(Stage[TransactionData]):
    def process(self, item: Item[TransactionData]) -> Item[TransactionData]:
        data = item.data
        data["decision"] = "review" if data.get("score", 0.0) >= 0.5 else "allow"
        submitted = item.metadata.get("submitted_at")
        if submitted is not None:
            data["latency_ms"] = (time.perf_counter() - submitted) * 1000.0
        return item


def make_transaction(index: int, rng: random.Random) -> Item[TransactionData]:
    amount = round(rng.lognormvariate(4.4, 1.0), 2)
    item = Item[TransactionData](
        {
            "txn_id": f"txn-{index:06d}",
            "amount": amount,
            "currency": rng.choice(("EUR", "USD", "GBP")),
            "merchant": f"merchant-{rng.randrange(500)}",
            "country": rng.choice(COUNTRIES),
            "device": rng.choice(DEVICES),
            "hour": rng.randrange(24),
            "merchant_risk": 0.5,
            "velocity": 0.2,
            "score": 0.0,
            "decision": "",
            "latency_ms": 0.0,
            "features": [],
        }
    )
    item.metadata["submitted_at"] = time.perf_counter()
    return item


def producer(
    pipeline: Pipeline[TransactionData],
    total: int,
    in_flight: threading.BoundedSemaphore,
    stop: threading.Event,
    rng: random.Random,
    on_done,
) -> None:
    submitted = 0
    try:
        for index in range(total):
            if stop.is_set():
                break
            in_flight.acquire()
            item = make_transaction(index, rng)
            try:
                pipeline.process_async(item, callback=on_done)
            except PipelineStateError:
                in_flight.release()
                break
            submitted += 1
            # bursty arrivals, with a tiny pause every so often
            if index % 400 == 399:
                time.sleep(0.01)
        pipeline.stop()
    except BaseException:
        stop.set()
        raise
    finally:
        print(f"submitted={submitted}")


def build_pipeline(args: argparse.Namespace) -> Pipeline[TransactionData]:
    return (
        Pipeline[TransactionData](max_queues_size=args.queue_size)
        .append("validate", Validate())
        .append(
            "enrich",
            MerchantEnrichment(),
            concurrency=max(1, args.workers),
            retryable_errors=(GatewayTimeout,),
            max_retries=2,
            backoff=0.05,
        )
        .append("score", RiskModel(size=64), concurrency=2)
        .append("decide", Decide())
        .build()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_scale_args(
        parser, items=6000, item_label="transactions", workers=4, queue_size=256
    )
    parser.add_argument(
        "--in-flight",
        type=int,
        default=400,
        help="max submitted but not yet delivered items (default: %(default)s)",
    )
    args = parser.parse_args()
    configure_logging()

    pipeline = build_pipeline(args)
    metrics = RunMetrics()
    in_flight = threading.BoundedSemaphore(args.in_flight)
    stop = threading.Event()
    rng = random.Random(23)

    def on_done(item: Item[TransactionData]) -> None:
        in_flight.release()

    # every submitted item gets the callback; it runs on the consumer thread
    producer_thread = threading.Thread(
        target=producer,
        args=(pipeline, args.items, in_flight, stop, rng, on_done),
        daemon=True,
    )

    latencies: List[float] = []
    retries = 0
    reviews = 0
    run_start = time.perf_counter()
    producer_thread.start()
    # get_item() waits for the producer's first process_async() to start the run
    failure: BaseException | None = None
    try:
        while True:
            try:
                item = pipeline.get_item()
            except PipelineStateError:
                break
            metrics.add(item)
            if item.data["decision"] == "review":
                reviews += 1
            retries += max(0, int(item.metadata.get("enrich_attempts", 1)) - 1)
            latencies.append(item.data["latency_ms"])
    except BaseException as error:
        failure = error
    finally:
        stop.set()
        producer_thread.join(timeout=10)
        pipeline.close()
    elapsed = time.perf_counter() - run_start
    if failure is not None:
        raise failure

    latencies.sort()
    p50 = latencies[len(latencies) // 2] if latencies else 0.0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0
    model = pipeline.get_stage("score")
    assert isinstance(model, RiskModel)
    metrics.report(
        "Fraud stream",
        elapsed,
        extra=[
            f"in_flight_limit={args.in_flight} queue_size={args.queue_size} "
            f"workers={args.workers}",
            f"reviews={reviews} retried_items={retries} "
            f"model_batches={model.batches}",
            f"latency_p50={p50:.1f}ms latency_p95={p95:.1f}ms "
            f"latency_max={latencies[-1] if latencies else 0:.1f}ms",
            f"count_property={pipeline.count}",
        ],
    )


if __name__ == "__main__":
    main()
