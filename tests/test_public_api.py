import smartpipeline

__author__ = "Giacomo Berardi <giacbrd.com>"


def test_top_level_exports():
    """The public names are exported from the package top level."""
    assert smartpipeline.__all__ == [
        "Pipeline",
        "Stage",
        "BatchStage",
        "Source",
        "Item",
        "ErrorManager",
        "SoftError",
        "CriticalError",
    ]
    from smartpipeline import (
        BatchStage,
        CriticalError,
        ErrorManager,
        Item,
        Pipeline,
        SoftError,
        Source,
        Stage,
    )
    from smartpipeline.error.exceptions import CriticalError as SubCriticalError
    from smartpipeline.error.exceptions import SoftError as SubSoftError
    from smartpipeline.error.handling import ErrorManager as SubErrorManager
    from smartpipeline.item import Item as SubItem
    from smartpipeline.pipeline import Pipeline as SubPipeline
    from smartpipeline.stage import BatchStage as SubBatchStage
    from smartpipeline.stage import Source as SubSource
    from smartpipeline.stage import Stage as SubStage

    assert Pipeline is SubPipeline
    assert Stage is SubStage
    assert BatchStage is SubBatchStage
    assert Source is SubSource
    assert Item is SubItem
    assert ErrorManager is SubErrorManager
    assert SoftError is SubSoftError
    assert CriticalError is SubCriticalError
