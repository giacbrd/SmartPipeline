import pytest
from tests.utils import TEXT_SAMPLES

__author__ = 'Giacomo Berardi <giacbrd.com>'


@pytest.fixture(scope='session')
def text_samples_fx():
    yield TEXT_SAMPLES

