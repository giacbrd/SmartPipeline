import itertools
import os
from tempfile import TemporaryDirectory
from typing import Any, Dict

import pytest

from smartpipeline.item import Item
from tests.utils import TEXT_SAMPLES, random_text

__author__ = "Giacomo Berardi <giacbrd.com>"


@pytest.fixture(scope="session")
def text_samples_fx():
    yield TEXT_SAMPLES


@pytest.fixture
def items_generator_fx():
    def _generator():
        for i in itertools.count():
            item = Item[Dict[str, Any]]({"id": i, "text": random_text()})
            yield item

    yield _generator()


@pytest.fixture
def file_directory_source_fx():
    with TemporaryDirectory() as temp:
        for text in TEXT_SAMPLES:
            with open(os.path.join(temp, str(hash(text))), "w") as f:
                f.write(text)
        yield temp
