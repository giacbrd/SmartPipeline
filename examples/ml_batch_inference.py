"""Batch ML inference: a recommendation team's scoring job.

Persona: an ML platform engineer scoring a large synthetic feature table with
a scikit-learn random forest, in batches, and writing predictions to JSONL.

Stresses:
- a real machine-learning library (numpy + scikit-learn) inside stages
- batch inference, where the model is far faster per row than per-call overhead
- both thread and process executors for a pickled model and numpy payloads
- a fast inline preprocessor between the source and the model
- a batch JSONL sink and a final inline report
- memory/latency trade-offs of a bounded queue with many small feature vectors

This example needs the optional examples dependencies::

    pip install numpy scikit-learn

Run from the repository root::

    python examples/ml_batch_inference.py --items 50000 --workers 2
    python examples/ml_batch_inference.py --items 20000 --executor processes
"""

from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

import argparse
import json
import tempfile
import time
from pathlib import Path
from typing import Sequence, TypedDict

import numpy as np
from _common import RunMetrics, add_scale_args, configure_logging, consume
from sklearn.ensemble import RandomForestClassifier

from smartpipeline import BatchStage, Item, Pipeline, Source, Stage

FEATURES = 48
TRAINING_ROWS = 20_000


class RowData(TypedDict):
    row_id: int
    features: np.ndarray
    score: float
    label: int


def make_training_data(rows: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    features = rng.normal(size=(rows, FEATURES))
    weights = np.linspace(-1.0, 1.0, FEATURES)
    logits = features @ weights + 0.8 * rng.normal(size=rows)
    labels = (logits > 0).astype(np.int8)
    return features, labels


class FeatureStream(Source[RowData]):
    """Generate rows lazily: the full inference set is never kept in memory."""

    def __init__(self, rows: int, seed: int):
        self._rows = rows
        self._index = 0
        self._rng = np.random.default_rng(seed)

    def pop(self):
        if self._index >= self._rows:
            self.stop()
            return None
        self._index += 1
        return Item[RowData](
            {
                "row_id": self._index,
                "features": self._rng.normal(size=FEATURES),
                "score": 0.0,
                "label": 0,
            }
        )


class Preprocess(Stage[RowData]):
    """Cheap inline work: clip outliers and add a squared feature interaction."""

    def process(self, item: Item[RowData]) -> Item[RowData]:
        features = item.data["features"]
        features = np.clip(features, -4.0, 4.0)
        item.data["features"] = features
        return item


class BatchModel(BatchStage[RowData]):
    def __init__(self, model: RandomForestClassifier, size: int = 512):
        super().__init__(size, timeout=0.05)
        self._model = model

    def process_batch(self, items: Sequence[Item[RowData]]) -> Sequence[Item[RowData]]:
        matrix = np.vstack([item.data["features"] for item in items])
        scores = self._model.predict_proba(matrix)[:, 1]
        for item, score in zip(items, scores):
            item.data["score"] = float(score)
            item.data["label"] = int(score >= 0.5)
            # the feature vector is not needed by the sink: do not ship it back
            item.data["features"] = np.empty(0)
        return items


class PredictionSink(BatchStage[RowData]):
    def __init__(self, path: Path, size: int = 512, timeout: float = 0.2):
        super().__init__(size, timeout)
        self._path = path
        self._file = None
        self.rows = 0
        self.positive = 0

    def on_start(self) -> None:
        self._file = self._path.open("w", encoding="utf-8")

    def process_batch(self, items: Sequence[Item[RowData]]) -> Sequence[Item[RowData]]:
        assert self._file is not None
        for item in items:
            self._file.write(
                json.dumps(
                    {
                        "row_id": item.data["row_id"],
                        "score": round(item.data["score"], 4),
                        "label": item.data["label"],
                    }
                )
                + "\n"
            )
            self.rows += 1
            self.positive += item.data["label"]
        self._file.flush()
        return items

    def on_end(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


class Report(Stage[RowData]):
    def __init__(self) -> None:
        self.items = 0
        self.positive = 0
        self.score_sum = 0.0

    def process(self, item: Item[RowData]) -> Item[RowData]:
        self.items += 1
        self.positive += item.data["label"]
        self.score_sum += item.data["score"]
        return item


def build_pipeline(
    args: argparse.Namespace,
    model: RandomForestClassifier,
    output: Path,
) -> Pipeline[RowData]:
    return (
        Pipeline[RowData](max_queues_size=args.queue_size)
        .set_source(FeatureStream(args.items, seed=31))
        .append("preprocess", Preprocess())
        .append(
            "model",
            BatchModel(model, size=args.batch_size),
            concurrency=max(1, args.workers),
            executor=args.executor,
        )
        .append("sink", PredictionSink(output), concurrency=1)
        .append("report", Report())
        .build()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_scale_args(
        parser, items=50_000, item_label="feature rows", workers=2, queue_size=128
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=512,
        help="rows per model prediction (default: %(default)s)",
    )
    parser.add_argument(
        "--trees",
        type=int,
        default=24,
        help="random-forest trees (default: %(default)s)",
    )
    parser.add_argument(
        "--executor",
        choices=("threads", "processes"),
        default="threads",
        help="executor of the batch model (default: %(default)s)",
    )
    args = parser.parse_args()
    configure_logging()

    train_start = time.perf_counter()
    train_x, train_y = make_training_data(TRAINING_ROWS, seed=7)
    model = RandomForestClassifier(
        n_estimators=args.trees,
        max_depth=10,
        n_jobs=1,
        random_state=7,
    )
    model.fit(train_x, train_y)
    training = time.perf_counter() - train_start

    with tempfile.TemporaryDirectory(prefix="smartpipeline-ml-") as tmp:
        output = Path(tmp) / "predictions.jsonl"
        pipeline = build_pipeline(args, model, output)
        metrics = RunMetrics()
        elapsed = consume(pipeline, metrics)

        sink = pipeline.get_stage("sink")
        report = pipeline.get_stage("report")
        assert isinstance(sink, PredictionSink)
        assert isinstance(report, Report)
        metrics.report(
            "Batch ML inference",
            elapsed,
            extra=[
                f"executor={args.executor} workers={args.workers} "
                f"batch_size={args.batch_size} queue_size={args.queue_size}",
                f"model=RandomForest({args.trees} trees) "
                f"trained_on={TRAINING_ROWS} rows in {training:.2f}s",
                f"sink_rows={sink.rows} sink_positive={sink.positive} "
                f"report_items={report.items}",
                f"report_positive={report.positive} "
                f"avg_score={report.score_sum / max(1, report.items):.4f}",
                f"output_bytes={output.stat().st_size if output.exists() else 0}",
            ],
        )


if __name__ == "__main__":
    main()
