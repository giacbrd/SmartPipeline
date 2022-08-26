import copy
import logging
import time
import traceback

import pytest

from smartpipeline.error.exceptions import RetryError
from tests.utils import (
    CriticalIOErrorStage,
    CustomizableBrokenStage,
    ErrorSerializableStage,
    ErrorStage,
    ExceptionStage,
    Logger,
    RandomTextSource,
    SerializableErrorManager,
    SerializableStage,
    TextDuplicator,
    TextReverser,
    TimeWaster,
    get_pipeline,
)

__author__ = "Giacomo Berardi <giacbrd.com>"

logger = logging.getLogger(__name__)


def _check(items, num, pipeline=None):
    diff = frozenset(range(1, num + 1)).difference(
        item.payload["count"] for item in items
    )
    assert not diff, "Not found items: {}".format(", ".join(str(x) for x in diff))
    assert len(items) == num
    if pipeline:
        assert num == pipeline.count


def test_concurrent_run():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2)
        .append_stage("reverser1", TextReverser(), concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2, parallel=True)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser2", TextReverser(), concurrency=0)
        .append_stage("duplicator", TextDuplicator(), concurrency=2, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=0)
        .append_stage("reverser1", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=0)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=False)
        .append_stage("duplicator", TextDuplicator(), concurrency=1, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("duplicator0", TextDuplicator(), concurrency=0)
        .append_stage("reverser", TextReverser(), concurrency=0)
        .append_stage("duplicator1", TextDuplicator(), concurrency=0)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser", TextReverser(), concurrency=0)
        .append_stage("duplicator0", TextDuplicator(), concurrency=0)
        .append_stage("duplicator1", TextDuplicator(), concurrency=1)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)


def test_queue_sizes():
    pipeline = (
        get_pipeline(max_queues_size=1)
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2)
        .append_stage("reverser1", TextReverser(), concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline(max_queues_size=1)
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2, parallel=True)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser2", TextReverser(), concurrency=0)
        .append_stage("duplicator", TextDuplicator(), concurrency=2, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline(max_queues_size=3)
        .set_source(RandomTextSource(100))
        .append_stage("waster", TimeWaster(0.02), concurrency=2)
        .append_stage("reverser", TimeWaster(0.04), concurrency=1, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline(max_queues_size=0)
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2)
        .append_stage("reverser1", TextReverser(), concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline(max_queues_size=0)
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=2, parallel=True)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser2", TextReverser(), concurrency=0)
        .append_stage("duplicator", TextDuplicator(), concurrency=2, parallel=True)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)


def test_concurrency_errors(caplog):
    with pytest.raises(Exception):
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(10))
            .append_stage("reverser", TextReverser(), concurrency=1)
            .append_stage("error", ExceptionStage(), concurrency=1)
            .build()
        )
        try:
            for _ in pipeline.run():
                pass
        except Exception:
            tb = traceback.format_exc()
            assert 'Exception("test exception")' in tb
            assert "CriticalError" in tb
            raise
        assert (
            len(
                [
                    record
                    for record in caplog.records
                    if record.levelno == logging.ERROR
                    and "has generated an error" in record.msg.lower()
                ]
            )
            == 1
        )
        assert pipeline.count == 1
    caplog.clear()
    with pytest.raises(Exception):
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(10))
            .append_stage("reverser", TextReverser(), concurrency=1, parallel=True)
            .append_stage("error", ExceptionStage(), concurrency=1, parallel=True)
            .build()
        )
        try:
            for _ in pipeline.run():
                pass
        except Exception:
            tb = traceback.format_exc()
            assert 'Exception("test exception")' in tb
            assert "CriticalError" in tb
            raise
        assert (
            len(
                [
                    record
                    for record in caplog.records
                    if record.levelno == logging.ERROR
                    and "has generated an error" in record.msg.lower()
                ]
            )
            == 1
        )
        assert pipeline.count == 1
    caplog.clear()
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("reverser", TextReverser(), concurrency=1)
        .append_stage("error1", ErrorStage(), concurrency=1, parallel=True)
        .append_stage("error2", ErrorStage(), concurrency=2)
        .append_stage("duplicator", TextDuplicator(), concurrency=1, parallel=True)
        .build()
    )
    for item in pipeline.run():
        assert item.get_timing("reverser")
        assert item.get_timing("duplicator")
        assert any(k.startswith("text_") for k in item.payload.keys())
        assert len(list(item.soft_errors())) == 2
        assert item.get_timing("error1")
        assert item.get_timing("error2")
    assert (
        len(
            [
                record
                for record in caplog.records
                if record.levelno == logging.ERROR
                and "has generated an error" in record.msg.lower()
            ]
        )
        == 20
    )
    assert pipeline.count == 10


def test_concurrent_constructions():
    """test `on_start` and `on_end` methods"""
    serializable_stage = SerializableStage()
    error_manager = SerializableErrorManager()
    pipeline = (
        get_pipeline()
        .set_error_manager(error_manager)
        .set_source(RandomTextSource(10))
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("stage", serializable_stage, concurrency=2, parallel=True)
        .append_stage("reverser2", TextReverser())
        .append_stage("error", ErrorStage(), concurrency=2, parallel=True)
        .build()
    )
    for item in pipeline.run():
        assert serializable_stage._file is None
        assert item.payload.get("file")
        assert item.get_timing("reverser1")
        assert item.get_timing("reverser2")
        assert item.has_errors()
    assert pipeline.count == 10
    assert serializable_stage._file is None
    assert error_manager.is_closed()
    serializable_stage = SerializableStage()
    error_manager = SerializableErrorManager()
    pipeline = (
        get_pipeline()
        .set_error_manager(error_manager)
        .set_source(RandomTextSource(10))
        .append_stage("stage", serializable_stage)
        .append_stage("reverser1", TextReverser(), concurrency=1, parallel=True)
        .append_stage("reverser2", TextReverser(), concurrency=1, parallel=True)
        .append_stage("error", ErrorStage())
        .build()
    )
    for item in pipeline.run():
        assert item.payload.get("file")
        assert item.get_timing("reverser1")
        assert item.get_timing("reverser2")
        assert item.has_errors()
    assert pipeline.count == 10
    assert serializable_stage.is_closed()
    assert error_manager.is_closed()
    with pytest.raises(IOError):
        try:
            pipeline = get_pipeline()
            (
                pipeline.set_error_manager(SerializableErrorManager())
                .set_source(RandomTextSource(10))
                .append_stage(
                    "stage", ErrorSerializableStage(), concurrency=1, parallel=True
                )
                .build()
            )
        finally:
            pipeline._stop_logs_receiver()
    error_manager = SerializableErrorManager().raise_on_critical_error()
    with pytest.raises(IOError):
        pipeline = (
            get_pipeline()
            .set_error_manager(error_manager)
            .set_source(RandomTextSource(10))
            .append_stage("stage", CriticalIOErrorStage(), concurrency=1, parallel=True)
            .build()
        )
        for item in pipeline.run():
            pass
    assert error_manager.is_closed()


def test_concurrent_initialization():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage_concurrently(
            "reverser0", TextReverser, kwargs={"cycles": 3}, concurrency=2
        )
        .append_stage_concurrently("reverser1", TextReverser, args=[5], concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage_concurrently(
            "reverser0", TextReverser, concurrency=2, parallel=True
        )
        .append_stage_concurrently(
            "reverser1", TextReverser, args=[10], concurrency=1, parallel=True
        )
        .append_stage_concurrently("reverser2", TextReverser, concurrency=0)
        .append_stage_concurrently(
            "duplicator", TextDuplicator, args=[10], concurrency=2, parallel=True
        )
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser0", TextReverser(), concurrency=0)
        .append_stage_concurrently("reverser1", TextReverser, concurrency=1)
        .append_stage("duplicator", TextDuplicator(10), concurrency=0)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage_concurrently("duplicator0", TextDuplicator, concurrency=0)
        .append_stage("reverser", TextReverser(), concurrency=0)
        .append_stage_concurrently("duplicator1", TextDuplicator, concurrency=0)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(100))
        .append_stage("reverser", TextReverser(12), concurrency=0)
        .append_stage_concurrently("duplicator0", TextDuplicator, concurrency=0)
        .append_stage("duplicator1", TextDuplicator(), concurrency=1)
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)
    pipeline = (
        get_pipeline(max_init_workers=1)
        .set_source(RandomTextSource(100))
        .append_stage_concurrently(
            "reverser0", TextReverser, args=[20], concurrency=1, parallel=True
        )
        .append_stage_concurrently(
            "reverser1", TextReverser, args=[20], concurrency=1, parallel=False
        )
        .append_stage_concurrently(
            "duplicator", TextDuplicator, args=[20], concurrency=1, parallel=True
        )
        .build()
    )
    items = list(pipeline.run())
    _check(items, 100, pipeline)


# one core machines can have problems with this test
# TODO it has problems also on dual core machine
@pytest.mark.skip
def test_huge_run():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(200))
        .append_stage("reverser0", TextReverser(15000), concurrency=4, parallel=True)
        .append_stage("reverser1", TextReverser(15000), concurrency=4, parallel=True)
        .append_stage("reverser2", TextReverser(15000), concurrency=4, parallel=True)
        .append_stage("duplicator", TextDuplicator(15000), concurrency=4, parallel=True)
        .build()
    )
    runner = pipeline.run()
    start_time = time.time()
    items = list(runner)
    elapsed1 = time.time() - start_time
    logger.debug("Time for strongly parallel: %s", elapsed1)
    _check(items, 200)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(200))
        .append_stage("reverser0", TextReverser(15000), concurrency=1, parallel=True)
        .append_stage("reverser1", TextReverser(15000), concurrency=2, parallel=True)
        .append_stage("reverser2", TextReverser(15000), concurrency=1, parallel=True)
        .append_stage("duplicator", TextDuplicator(15000), concurrency=1, parallel=True)
        .build()
    )
    runner = pipeline.run()
    start_time = time.time()
    items = list(runner)
    elapsed2 = time.time() - start_time
    logger.debug("Time for mildly parallel: %s", elapsed2)
    _check(items, 200)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(200))
        .append_stage("reverser0", TextReverser(15000), concurrency=0)
        .append_stage("reverser1", TextReverser(15000), concurrency=0)
        .append_stage("reverser2", TextReverser(15000), concurrency=0)
        .append_stage("duplicator", TextDuplicator(15000), concurrency=0)
        .build()
    )
    runner = pipeline.run()
    start_time = time.time()
    items = list(runner)
    elapsed3 = time.time() - start_time
    logger.debug("Time for sequential: %s", elapsed3)
    _check(items, 200)
    assert elapsed3 > elapsed1
    assert elapsed3 > elapsed2
    assert elapsed2 > elapsed1


def test_run_times():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("waster0", TimeWaster(0.2), concurrency=1)
        .append_stage("waster1", TimeWaster(0.2), concurrency=1)
        .append_stage("waster2", TimeWaster(0.2), concurrency=1)
        .append_stage("waster3", TimeWaster(0.2), concurrency=1)
        .build()
    )
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elapsed0 = time.time() - start_time
    logger.debug("Time for multi-threading: %s", elapsed0)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("waster0", TimeWaster(0.2), concurrency=1, parallel=True)
        .append_stage("waster1", TimeWaster(0.2), concurrency=1, parallel=True)
        .append_stage("waster2", TimeWaster(0.2), concurrency=1, parallel=True)
        .append_stage("waster3", TimeWaster(0.2), concurrency=1, parallel=True)
        .build()
    )
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elapsed1 = time.time() - start_time
    logger.debug("Time for multi-process: %s", elapsed1)
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(10))
        .append_stage("waster0", TimeWaster(0.2), concurrency=0)
        .append_stage("waster1", TimeWaster(0.2), concurrency=0)
        .append_stage("waster2", TimeWaster(0.2), concurrency=0)
        .append_stage("waster3", TimeWaster(0.2), concurrency=0)
        .build()
    )
    start_time = time.time()
    items = list(pipeline.run())
    _check(items, 10)
    elapsed2 = time.time() - start_time
    logger.debug("Time for sequential: %s", elapsed2)
    assert elapsed2 > elapsed0
    assert elapsed2 > elapsed1


def test_single_items(items_generator_fx):
    pipeline = (
        get_pipeline()
        .append_stage("reverser0", TextReverser())
        .append_stage("reverser1", TextReverser())
        .append_stage("reverser2", TextReverser())
        .append_stage("duplicator", TextDuplicator())
        .build()
    )
    item = next(items_generator_fx)
    result = pipeline.process(copy.deepcopy(item))
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        get_pipeline()
        .append_stage_concurrently(
            "reverser0", TextReverser, kwargs={"cycles": 3}, concurrency=2
        )
        .append_stage_concurrently("reverser1", TextReverser, args=[5], concurrency=0)
        .append_stage("reverser2", TextReverser(), concurrency=1)
        .append_stage("duplicator", TextDuplicator(), concurrency=2)
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        get_pipeline()
        .append_stage_concurrently(
            "reverser0", TextReverser, concurrency=2, parallel=True
        )
        .append_stage_concurrently(
            "reverser1", TextReverser, args=[9], concurrency=1, parallel=True
        )
        .append_stage_concurrently("reverser2", TextReverser, concurrency=0)
        .append_stage_concurrently(
            "duplicator", TextDuplicator, args=[10], concurrency=2, parallel=True
        )
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(item)
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        get_pipeline()
        .append_stage("reverser0", TextReverser(), concurrency=0)
        .append_stage_concurrently("reverser1", TextReverser, concurrency=1)
        .append_stage("duplicator", TextDuplicator(10), concurrency=0)
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] == item.payload["text"]

    pipeline = (
        get_pipeline()
        .append_stage_concurrently("duplicator0", TextDuplicator)
        .append_stage("reverser", TextReverser())
        .append_stage_concurrently("duplicator1", TextDuplicator)
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        get_pipeline()
        .append_stage("reverser", TextReverser(11), concurrency=0)
        .append_stage_concurrently("duplicator0", TextDuplicator, concurrency=0)
        .append_stage("duplicator1", TextDuplicator(), concurrency=1)
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(copy.deepcopy(item))
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] != item.payload["text"]

    pipeline = (
        get_pipeline(max_init_workers=1)
        .append_stage_concurrently(
            "reverser0", TextReverser, args=[20], concurrency=1, parallel=True
        )
        .append_stage_concurrently(
            "reverser1", TextReverser, args=[20], concurrency=1, parallel=False
        )
        .append_stage_concurrently(
            "duplicator", TextDuplicator, args=[20], concurrency=1, parallel=True
        )
        .build()
    )
    item = next(items_generator_fx)
    pipeline.process_async(item)
    result = pipeline.get_item()
    pipeline.stop()
    assert result.id == item.id
    assert result.payload["text"] == item.payload["text"]
    assert len(result.payload.keys()) > len(item.payload.keys())


def test_concurrent_run_with_retryable_stages():
    pipeline = (
        get_pipeline()
        .set_source(RandomTextSource(2))
        .append_stage("reverser0", TextReverser(), concurrency=2)
        .append_stage(
            "broken_stage1",
            CustomizableBrokenStage([ValueError]),
            concurrency=2,
            backoff=1,
            max_retries=2,
            retryable_errors=(ValueError,),
        )
        .append_stage(
            "broken_stage2",
            CustomizableBrokenStage([TypeError]),
            concurrency=2,
            parallel=True,
            backoff=1,
            max_retries=1,
            retryable_errors=(TypeError,),
        )
        .build()
    )
    items = list(pipeline.run())
    for item in items:
        assert not item.has_critical_errors()
        soft_errors = list(item.soft_errors())
        soft_errors_broken_stage_1 = [
            err for err in soft_errors if err.get_stage() == "broken_stage1"
        ]
        soft_errors_broken_stage_2 = [
            err for err in soft_errors if err.get_stage() == "broken_stage2"
        ]
        assert len(soft_errors_broken_stage_1) == 3 and all(
            isinstance(err, RetryError) and isinstance(err.get_exception(), ValueError)
            for err in soft_errors_broken_stage_1
        )
        assert len(soft_errors_broken_stage_2) == 2 and all(
            isinstance(err, RetryError) and isinstance(err.get_exception(), TypeError)
            for err in soft_errors_broken_stage_2
        )


def test_broken_loop():
    def _get_pipeline():
        return (
            get_pipeline()
            .set_source(RandomTextSource(100))
            .append_stage("reverser0", TextReverser(), concurrency=3, parallel=True)
            .append_stage("reverser1", TextReverser(), concurrency=2, parallel=False)
            .append_stage("duplicator", TextDuplicator(), concurrency=1, parallel=True)
            .build()
        )

    items = []
    for n, item in enumerate(_get_pipeline().run()):
        items.append(item)
        if n >= 9:
            break
    assert len(items) == 10
    items = []
    try:
        for n, item in enumerate(_get_pipeline().run()):
            items.append(item)
            if n >= 9:
                raise IOError
    except IOError:
        assert len(items) == 10


def test_logging(caplog):
    with caplog.at_level(logging.INFO):
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(100))
            .append_stage("logger1", Logger(), concurrency=2)
            .append_stage("reverser0", TextReverser(), concurrency=1)
            .build()
        )
        list(pipeline.run())
        messages = [
            record for record in caplog.records if record.levelno == logging.INFO
        ]
        assert len(messages) == 100
        assert all("logging item " in record.msg.lower() for record in messages)
        caplog.clear()
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(10))
            .append_stage_concurrently("logger1", Logger, concurrency=1, parallel=True)
            .append_stage("reverser0", TextReverser(), concurrency=1)
            .build()
        )
        list(pipeline.run())
        messages = [
            record for record in caplog.records if record.levelno == logging.INFO
        ]
        assert len(messages) == 10
        assert all("logging item " in record.msg.lower() for record in messages)
        assert any(
            "testing concurrent initialization" in record.msg.lower()
            for record in caplog.records
        )
        caplog.clear()
        pipeline = (
            get_pipeline()
            .set_source(RandomTextSource(100))
            .append_stage("reverser1", TextReverser(), concurrency=2)
            .append_stage("logger2", Logger(), concurrency=2, parallel=True)
            .append_stage("reverser2", TextReverser())
            .build()
        )
        list(pipeline.run())
        messages = [
            record for record in caplog.records if record.levelno == logging.INFO
        ]
        assert len(messages) == 100
        assert all("logging item " in record.msg.lower() for record in messages)
