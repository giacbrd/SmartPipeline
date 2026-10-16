import threading
import time
from queue import Queue

from smartpipeline.utils import put_or_abort, raise_collected_errors

__author__ = "Giacomo Berardi <giacbrd.com>"

import pytest

from tests.utils import text_item


def test_put_or_abort_success():
    """put_or_abort enqueues the item when the queue has room."""
    q: Queue = Queue(maxsize=1)
    assert put_or_abort(q, text_item(), timeout=0.01) is True
    assert q.qsize() == 1


def test_put_or_abort_aborts_when_guard_set():
    """put_or_abort drops the item once the fatal guard is set."""
    q: Queue = Queue(maxsize=1)
    q.put(text_item())
    assert put_or_abort(q, text_item(), lambda: True, timeout=0.01) is False
    assert q.qsize() == 1


def test_put_or_abort_retries_until_space():
    """put_or_abort waits for room instead of dropping when not fatal."""
    q: Queue = Queue(maxsize=1)
    q.put(text_item())

    def _free():
        time.sleep(0.05)
        q.get_nowait()
        q.task_done()

    releaser = threading.Thread(target=_free, daemon=True)
    releaser.start()
    assert put_or_abort(q, text_item(), lambda: False, timeout=0.01) is True
    releaser.join(timeout=2)


def test_raise_collected_errors():
    """No errors pass silently, one re-raises as-is, many group into ExceptionGroup."""
    assert raise_collected_errors([], "nothing") is None
    error = RuntimeError("single")
    with pytest.raises(RuntimeError, match="single"):
        raise_collected_errors([error], "single")
    with pytest.raises(ExceptionGroup, match="many") as exc_info:
        raise_collected_errors([RuntimeError("a"), ValueError("b")], "many")
    assert len(exc_info.value.exceptions) == 2
