import pytest

from smartpipeline.error.exceptions import CriticalError, SoftError
from smartpipeline.item import Item
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


def test_default_dict_payload():
    """A bare Item defaults to an empty dict payload."""
    item = Item()
    item.data["text"] = "hello"
    assert item.data["text"] == "hello"
