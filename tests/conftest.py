import itertools
import os
from tempfile import TemporaryDirectory

import pytest

from tests.utils import TEXT_SAMPLES, text_item

__author__ = "Giacomo Berardi <giacbrd.com>"


@pytest.fixture
def items_generator_fx():
    def _generator():
        for i in itertools.count():
            item = text_item()
            item.data["id"] = i
            yield item

    yield _generator()


@pytest.fixture
def file_directory_source_fx():
    with TemporaryDirectory() as temp:
        for text in TEXT_SAMPLES:
            with open(os.path.join(temp, str(hash(text))), "w") as f:
                f.write(text)
        yield temp
