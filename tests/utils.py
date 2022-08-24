import random
import time
from datetime import datetime
from time import sleep
from typing import List, Type

from smartpipeline.error.exceptions import CriticalError, SoftError
from smartpipeline.error.handling import ErrorManager
from smartpipeline.helpers import FilePathItem
from smartpipeline.item import DataItem
from smartpipeline.pipeline import Pipeline
from smartpipeline.stage import BatchStage, Source, Stage

__author__ = "Giacomo Berardi <giacbrd.com>"

TEXT_SAMPLES = (
    "On the other hand, we denounce with righteous indignation and dislike men who are so beguiled and demoralized by the charms of pleasure of the moment,",
    "so blinded by desire, that they cannot foresee the pain and trouble that are bound to ensue; and equal blame belongs to those who fail in their duty through weakness of will,",
    "which is the same as saying through shrinking from toil and pain. These cases are perfectly simple and easy to distinguish.",
    "In a free hour, when our power of choice is untrammelled and when nothing prevents our being able to do what we like best, every pleasure is to be welcomed and every pain avoided.",
    "But in certain circumstances and owing to the claims of duty or the obligations of business it will frequently occur that pleasures have to be repudiated and annoyances accepted.",
    "The wise man therefore always holds in these matters to this principle of selection: he rejects pleasures to secure other greater pleasures, or else he endures pains to avoid worse pains.",
)


def random_text():
    return random.choice(TEXT_SAMPLES)


class CustomException(Exception):
    pass


class RandomTextSource(Source):
    def __init__(self, count):
        self.total = count
        self.counter = 0

    def pop(self):
        self.counter += 1
        if self.counter > self.total:
            self.stop()
            return
        item = DataItem()
        item.payload.update({"text": random_text(), "count": self.counter})
        return item


class ListSource(Source):
    def __init__(self, items):
        self.items = iter(items)

    def pop(self):
        try:
            return next(self.items)
        except StopIteration:
            self.stop()


class TextGenerator(Stage):
    def process(self, item: DataItem):
        item.payload["text"] = random_text()
        return item


class TextReverser(Stage):
    def __init__(self, cycles=1):
        self._cycles = cycles

    def process(self, item: DataItem):
        for _ in range(self._cycles):
            item.payload["text"] = item.payload["text"][::-1]
        return item


class TextDuplicator(Stage):
    def __init__(self, cycles=1):
        self._cycles = cycles

    def process(self, item: DataItem):
        for _ in range(self._cycles):
            item.payload["text_" + str(random.randint(1, 1000))] = item.payload["text"]
        return item


class CustomizableBrokenStage(Stage):
    def __init__(self, exceptions_to_raise: List[Type[Exception]]):
        self._exceptions_to_raise = exceptions_to_raise
        self._last_processed = 0

    def process(self, item: DataItem):
        to_raise = self._exceptions_to_raise[
            self._last_processed % len(self._exceptions_to_raise)
        ]
        self._last_processed += 1
        raise to_raise("exception")


class TextExtractor(Stage):
    def process(self, item: FilePathItem):
        with open(item.path) as f:
            item.payload["text"] = f.read()
        return item


class BatchTextGenerator(BatchStage):
    def __init__(self, size=10, timeout=0.1):
        super().__init__(size, timeout)

    def process_batch(self, items):
        for item in items:
            item.payload["text"] = random_text()
        return items


class BatchTextReverser(BatchStage):
    def __init__(self, cycles=1, size=10, timeout=0.1):
        super().__init__(size, timeout)
        self._cycles = cycles

    def process_batch(self, items):
        for item in items:
            for _ in range(self._cycles):
                item.payload["text"] = item.payload["text"][::-1]
        return items


class BatchTextDuplicator(BatchStage):
    def __init__(self, cycles=1, size=10, timeout=0.1, check_batch=False):
        super().__init__(size, timeout)
        self._check_batch = check_batch
        self._cycles = cycles

    def process_batch(self, items):
        if self._check_batch:
            if len(items) != self.size:
                raise CriticalError(
                    "The current batch contains {} items instead of {}".format(
                        len(items), self.size
                    )
                )
        for item in items:
            for c in range(self._cycles):
                item.payload[
                    "text_b_{}_{}".format(c, random.randint(1, 1000))
                ] = item.payload["text"]
        return items


class CustomizableBrokenBatchStage(BatchStage):
    def __init__(self, size, timeout, exceptions_to_raise: List[Type[Exception]]):
        super().__init__(size, timeout)
        self._exceptions_to_raise = exceptions_to_raise
        self._last_processed = 0

    def process_batch(self, items):
        to_raise = self._exceptions_to_raise[
            self._last_processed % len(self._exceptions_to_raise)
        ]
        self._last_processed += 1
        raise to_raise("exception")


class TimeWaster(Stage):
    def __init__(self, time=1):
        self._time = time

    def process(self, item: DataItem):
        time.sleep(self._time)
        return item


class ExceptionStage(Stage):
    def process(self, item: DataItem):
        time.sleep(0.3)
        raise Exception("test exception")


class ErrorStage(Stage):
    def process(self, item: DataItem):
        raise SoftError("test pipeline error")


class CriticalIOErrorStage(Stage):
    def process(self, item: DataItem):
        raise CriticalError("test pipeline critical IO error") from IOError()


class BatchExceptionStage(BatchStage):
    def __init__(self, size=10, timeout=0.1):
        super().__init__(size, timeout)

    def process_batch(self, items):
        time.sleep(0.3)
        raise Exception("test exception")


class BatchErrorStage(BatchStage):
    def __init__(self, size=10, timeout=0.1):
        super().__init__(size, timeout)

    def process_batch(self, items):
        raise SoftError("test pipeline error")


class SerializableStage(Stage):
    def __init__(self):
        self._file = None

    def on_start(self):
        self._file = open(__file__)

    def on_end(self):
        self._file.close()

    def is_closed(self):
        return self._file.closed

    def process(self, item: DataItem):
        if self._file is not None and self._file.name == __file__:
            item.payload["file"] = self._file.name
        return item


class ErrorSerializableStage(Stage):
    def __init__(self):
        self._file = None

    def on_start(self):
        raise IOError

    def process(self, item: DataItem):
        return item


class SerializableErrorManager(ErrorManager):
    def __init__(self):
        self._file = None

    def on_start(self):
        self._file = open(__file__)

    def on_end(self):
        self._file.close()

    def is_closed(self):
        return self._file.closed


def wait_service(timeout, predicate, args):
    start = datetime.now()
    while not predicate(*args):
        sleep(1)
        if (datetime.now() - start).seconds > timeout:
            raise TimeoutError()


def get_pipeline(*args, **kwargs):
    return Pipeline(*args, **kwargs).set_error_manager(
        ErrorManager().raise_on_critical_error()
    )
