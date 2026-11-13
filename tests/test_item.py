import pickle
from typing import Optional

import pytest

from smartpipeline.error.exceptions import CriticalError, SoftError
from smartpipeline.item import Item, Stop
from tests.utils import TextReverser

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_id_metadata_sync():
    """Item id follows the data id and metadata round-trips."""
    item = Item()
    assert item.id
    item.data["text"] = "prova"
    item.data["id"] = "666"
    item.metadata["source"] = "remote"
    item.metadata["version"] = 3
    assert set(item.metadata.keys()) == {"source", "version"}
    assert item.id == "666"
    assert item.metadata["source"] == "remote"
    assert item.metadata["version"] == 3
    assert not item.metadata.get("head")


def test_soft_critical_error_attachment():
    """Soft and critical errors attach to items, and mixing their types is rejected."""
    item = Item()
    stage = TextReverser()
    item.add_soft_error(stage.name, ValueError("value error"))
    item.add_soft_error(stage.name, KeyError("key error"))
    item.add_critical_error(stage.name, KeyError("key error"))
    assert item.has_critical_errors()
    assert item.has_soft_errors()
    assert len(list(item.soft_errors())) == 2
    assert len(list(item.critical_errors())) == 1
    stage = TextReverser()
    item.add_soft_error(stage.name, SoftError())
    item.add_critical_error(stage.name, CriticalError())
    with pytest.raises(ValueError):
        item.add_soft_error(stage.name, CriticalError())
    with pytest.raises(ValueError):
        item.add_critical_error(stage.name, SoftError())

    class CriticalSubclass(CriticalError):
        pass

    class SoftSubclass(SoftError):
        pass

    with pytest.raises(ValueError):
        item.add_soft_error(stage.name, CriticalSubclass())
    with pytest.raises(ValueError):
        item.add_critical_error(stage.name, SoftSubclass())


def test_soft_error_preserves_an_explicit_cause():
    """A SoftError keeps the cause set with ``raise ... from`` when it is attached."""
    item = Item()
    error = SoftError("wrapped error")
    cause = ValueError("original cause")
    error.__cause__ = cause
    attached = item.add_soft_error("stage", error)
    assert attached is error
    assert attached.get_exception() is cause


def test_default_dict_payload():
    """A bare Item defaults to an empty dict payload."""
    item = Item()
    item.data["text"] = "hello"
    assert item.data["text"] == "hello"


def test_none_is_a_valid_payload():
    """A None payload is kept as it is, it is not replaced by the default one."""
    item = Item[Optional[str]](None)
    assert item.data is None
    assert item.data_snippet() == "None"
    assert Item[str]("hello").data_snippet() == "hello"
    # the default payload is only used when the data is not given at all
    assert Item().data == {}
    assert Item({"a": 1}).data == {"a": 1}


def test_timed_stages_lists_the_stages_with_a_timing():
    """Only the stages which recorded a process time are listed."""
    item = Item()
    item.set_timing("reverser", 0.5)
    item.set_timing("generator", 0.25)
    assert set(item.timed_stages) == {"reverser", "generator"}
    assert item.get_timing("reverser") == 0.5
    assert not Item().timed_stages


def test_pickle_sends_the_callback_id_but_not_the_callback():
    """A pickled item keeps the key of its callback, but not the callback itself."""
    fired: list = []
    item = Item({"text": "hello"})
    key = item.set_callback(lambda it: fired.append(it.data["text"]))
    item.set_timing("reverser", 0.5)
    item.metadata["source"] = "remote"
    item.callback()
    assert fired == ["hello"]

    copy = pickle.loads(pickle.dumps(item))
    assert copy.callback_id == key
    assert copy.callback() is None
    # everything else, and the original item, are untouched
    assert fired == ["hello"]
    assert copy.data == {"text": "hello"}
    assert copy.get_timing("reverser") == 0.5
    assert copy.metadata["source"] == "remote"


def test_stop_is_a_str_signal_item():
    """The internal Stop sentinel reads as a stop signal carrying its id."""
    stop = Stop()
    assert str(stop) == f"Stop signal {stop.id}"
    assert isinstance(stop, Item)
