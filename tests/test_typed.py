from typing import Optional

from smartpipeline.item import Item
from smartpipeline.pipeline import Pipeline
from tests.utils import (
    OptionalNoOp,
    StrUpper,
    TypedDoc,
    TypedDocUpper,
    TypedRow,
    TypedRowSource,
    TypedRowUpper,
    run_with_timeout,
)

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_typed_dict_pipeline():
    """TypedDict payloads flow through a typed pipeline with id sync."""
    pipeline: Pipeline[TypedRow] = (
        Pipeline[TypedRow]()
        .set_source(TypedRowSource(5))
        .append("upper", TypedRowUpper())
        .build()
    )
    items = run_with_timeout(pipeline)
    assert len(items) == 5
    for item in items:
        assert item.data["text"].isupper()
        assert item.id == item.data["id"]


def test_dataclass_pipeline():
    """Dataclass payloads flow through process with id sync."""
    pipeline: Pipeline[TypedDoc] = (
        Pipeline[TypedDoc]().append("upper", TypedDocUpper()).build()
    )
    out = pipeline.process(Item[TypedDoc](TypedDoc(id="1", text="hello")))
    assert out.data.text == "HELLO"
    assert out.id == "1"


def test_primitive_process():
    """Plain str payloads flow through process."""
    pipeline: Pipeline[str] = Pipeline[str]().append("upper", StrUpper()).build()
    assert pipeline.process(Item[str]("hello")).data == "HELLO"


def test_optional_payload_process():
    """An Optional payload keeps its None value through the pipeline."""
    pipeline: Pipeline[Optional[str]] = (
        Pipeline[Optional[str]]().append("noop", OptionalNoOp()).build()
    )
    out = pipeline.process(Item[Optional[str]](None))
    assert out.data is None
    out = pipeline.process(Item[Optional[str]]("hello"))
    assert out.data == "hello"
