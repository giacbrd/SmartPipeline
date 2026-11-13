"""Genome k-mer spectra: a bioinformatics batch job.

Persona: a genomics researcher computing k-mer spectra and GC content for
a set of FASTA-like sequences.

Stresses:
- heavy pure-Python CPU work, where threads cannot escape the GIL
- ``executor="processes"`` with large string payloads (pickling both ways)
- a stage that drops the bulky field before returning, shrinking queue traffic
- a batch merge stage and a last inline report
- malformed sequences as soft errors and a quiet error manager

Run from the repository root::

    python examples/genome_kmers.py --items 160 --workers 4
    python examples/genome_kmers.py --items 40 --workers 4 --executor threads
"""

from __future__ import annotations

__author__ = "Giacomo Berardi <giacbrd.com>"

import argparse
import heapq
import json
import random
import tempfile
import time
from array import array
from collections import Counter
from pathlib import Path
from typing import Sequence, TypedDict

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

BASES = "ACGT"


class GenomeData(TypedDict):
    accession: str
    sequence: str
    bases: int
    gc: float
    unique_kmers: int
    max_count: int
    top_kmers: list[tuple[str, int]]


def make_records(count: int, length: int, seed: int = 5) -> list[GenomeData]:
    rng = random.Random(seed)
    records = []
    for index in range(count):
        sequence = "".join(rng.choices(BASES, k=length))
        if index in (7, 31):
            # invalid alphabet: validation must reject it
            half = length // 2
            sequence = sequence[:half] + "X" * 12 + sequence[half:]
        elif index == 13:
            sequence = ""
        records.append(
            GenomeData(
                accession=f"SEQ_{index:04d}",
                sequence=sequence,
                bases=len(sequence),
                gc=0.0,
                unique_kmers=0,
                max_count=0,
                top_kmers=[],
            )
        )
    return records


class GenomeSource(Source[GenomeData]):
    def __init__(self, records: Sequence[GenomeData]):
        self._records = list(records)
        self._index = 0

    def pop(self):
        if self._index >= len(self._records):
            self.stop()
            return None
        record = self._records[self._index]
        self._index += 1
        return Item[GenomeData](dict(record))


class Validate(Stage[GenomeData]):
    """Reject malformed records before they reach the expensive stage."""

    def process(self, item: Item[GenomeData]) -> Item[GenomeData]:
        sequence = item.data["sequence"]
        if not sequence:
            raise SoftError("empty sequence")
        if len(sequence) < 1_000:
            item.data["sequence"] = ""
            raise SoftError("sequence too short")
        if set(sequence) - set("ACGTN"):
            # release the payload before the item continues downstream
            item.data["sequence"] = ""
            raise SoftError("invalid nucleotide in sequence")
        item.data["bases"] = len(sequence)
        return item


class KmerSpectrum(Stage[GenomeData]):
    """Count k-mers with a rolling 2-bit code and a dense array."""

    def __init__(self, k: int = 9):
        self._k = k
        self._mask = (1 << (2 * k)) - 1
        self._size = 1 << (2 * k)

    def _decode(self, code: int) -> str:
        chars = []
        for shift in range(2 * (self._k - 1), -1, -2):
            chars.append(BASES[(code >> shift) & 3])
        return "".join(chars)

    def process(self, item: Item[GenomeData]) -> Item[GenomeData]:
        if item.has_soft_errors():
            return item
        sequence = item.data["sequence"]
        counts = array("I", [0]) * self._size
        code = 0
        valid = 0
        gc = 0
        for base in sequence:
            if base == "A":
                symbol = 0
            elif base == "C":
                symbol = 1
                gc += 1
            elif base == "G":
                symbol = 2
                gc += 1
            elif base == "T":
                symbol = 3
            else:
                code = 0
                valid = 0
                continue
            code = ((code << 2) | symbol) & self._mask
            valid += 1
            if valid >= self._k:
                counts[code] += 1
        top = heapq.nlargest(
            15,
            ((count, index) for index, count in enumerate(counts) if count),
            key=lambda entry: entry[0],
        )
        item.data["gc"] = round(gc / len(sequence), 4) if sequence else 0.0
        item.data["unique_kmers"] = sum(1 for count in counts if count)
        item.data["max_count"] = top[0][0] if top else 0
        item.data["top_kmers"] = [(self._decode(index), count) for count, index in top]
        # do not ship the 150KB sequence back through the queue
        item.data["sequence"] = ""
        return item


class MergeBatch(BatchStage[GenomeData]):
    """Merge per-sequence spectra and write one JSONL row per batch."""

    def __init__(self, path: Path, size: int = 16, timeout: float = 0.2):
        super().__init__(size, timeout)
        self._path = path
        self._file = None
        self._global: Counter[str] = Counter()
        self._batches = 0
        self._items = 0
        self._bases = 0
        self._unique = 0

    def on_start(self) -> None:
        self._file = self._path.open("w", encoding="utf-8")

    def process_batch(
        self, items: Sequence[Item[GenomeData]]
    ) -> Sequence[Item[GenomeData]]:
        assert self._file is not None
        batch: Counter[str] = Counter()
        bases = 0
        unique = 0
        for item in items:
            if item.has_soft_errors() or item.has_critical_errors():
                continue
            self._items += 1
            bases += item.data["bases"]
            unique += item.data["unique_kmers"]
            for kmer, count in item.data["top_kmers"]:
                batch[kmer] += count
                self._global[kmer] += count
        self._bases += bases
        self._unique += unique
        self._batches += 1
        self._file.write(
            json.dumps(
                {
                    "batch": self._batches,
                    "sequences": len(items),
                    "bases": bases,
                    "unique_kmers": unique,
                    "top": batch.most_common(5),
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


class FinalReport(Stage[GenomeData]):
    def __init__(self) -> None:
        self.items = 0
        self.failed = 0
        self.bases = 0

    def process(self, item: Item[GenomeData]) -> Item[GenomeData]:
        self.items += 1
        if item.has_soft_errors() or item.has_critical_errors():
            self.failed += 1
        else:
            self.bases += item.data["bases"]
        return item


class ExpectedErrors(ErrorManager):
    def __init__(self) -> None:
        self.soft = 0

    def handle(self, error, stage, item):
        if isinstance(error, SoftError):
            item.add_soft_error(stage.name, error)
            self.soft += 1
            return None
        return super().handle(error, stage, item)


def build_pipeline(
    records: Sequence[GenomeData],
    args: argparse.Namespace,
    output: Path,
    error_manager: ExpectedErrors,
) -> Pipeline[GenomeData]:
    return (
        Pipeline[GenomeData](max_queues_size=args.queue_size)
        .set_error_manager(error_manager)
        .set_source(GenomeSource(records))
        .append("validate", Validate())
        .append(
            "kmers",
            KmerSpectrum(k=9),
            concurrency=max(1, args.workers),
            executor=args.executor,
        )
        .append("merge", MergeBatch(output, size=16), concurrency=1)
        .append("report", FinalReport())
        .build()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_scale_args(parser, items=160, item_label="sequences", workers=4, queue_size=64)
    parser.add_argument(
        "--length",
        type=int,
        default=200_000,
        help="bases per sequence (default: %(default)s)",
    )
    parser.add_argument(
        "--executor",
        choices=("threads", "processes"),
        default="processes",
        help="executor of the k-mer stage (default: %(default)s)",
    )
    args = parser.parse_args()
    configure_logging()

    with tempfile.TemporaryDirectory(prefix="smartpipeline-genome-") as tmp:
        output = Path(tmp) / "spectra.jsonl"
        started = time.perf_counter()
        records = make_records(args.items, args.length)
        generation = time.perf_counter() - started
        total_bases = sum(record["bases"] for record in records)

        error_manager = ExpectedErrors()
        pipeline = build_pipeline(records, args, output, error_manager)
        metrics = RunMetrics()
        elapsed = consume(pipeline, metrics)

        merge = pipeline.get_stage("merge")
        report = pipeline.get_stage("report")
        assert isinstance(merge, MergeBatch)
        assert isinstance(report, FinalReport)
        metrics.report(
            "Genome k-mer spectra",
            elapsed,
            extra=[
                f"executor={args.executor} workers={args.workers} "
                f"queue_size={args.queue_size}",
                f"sequences={len(records)} length={args.length} "
                f"generated_in={generation:.2f}s",
                f"input={total_bases / 1e6:.1f}M bases "
                f"({total_bases / 1e6:.1f}MB strings)",
                f"merged_items={merge._items} batches={merge._batches} "
                f"bases={merge._bases / 1e6:.1f}M unique_kmers={merge._unique}",
                f"final_items={report.items} failed={report.failed} "
                f"soft_errors={error_manager.soft}",
                f"global_top={merge._global.most_common(5)}",
                f"output_rows={sum(1 for _ in output.open()) if output.exists() else 0}",
            ],
        )


if __name__ == "__main__":
    main()
