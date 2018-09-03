import copy
import json
import os
from shutil import copyfile
from tempfile import TemporaryDirectory, NamedTemporaryFile
from unittest.mock import patch

import pytest
from elasticsearch import Elasticsearch

from inpipeline.api.db import DocumentsPostgresClient
from inpipeline.config import DATATXT_ENDPOINT, ELASTICSEARCH_ENDPOINT, DB_PASSWORD, DB_USER, DB_NAME, DB_PORT, DB_HOST, \
    DB_GRID_TABLE, DB_DOCUMENTS_TABLE, INDEX_VERSION, PERCOLATOR_INDEX, DOCUMENTS_INDEX
from inpipeline.resources import resources_path
from inpipeline.stages.classification.build import index_queries, index_sample_documents
from inpipeline.stages.companies.annotator.search import Searcher
from inpipeline.stages.companies.grid.data import GRIDMap, GRIDFile
from inpipeline.stages.companies.grid.db import GRIDPostgresClient
from inpipeline.stages.index.utils import IndexManager, versioned
from tests.utils import wait_service, is_open, TEXT_SAMPLES, DATATXT_RESPONSE_SAMPLE, FAKE_SECURITY

__author__ = 'Giacomo Berardi <giacbrd.com>'


@pytest.fixture(scope='session')
def samples_fx():
    yield os.getenv("SAMPLES_PATH", "./samples")


def _es_client():
    host, port = ELASTICSEARCH_ENDPOINT.rsplit(':', 1) if ELASTICSEARCH_ENDPOINT else ('es', '9200')
    wait_service(60, is_open, [host, port])
    client = Elasticsearch('{}:{}'.format(host, port))
    return client


@pytest.fixture
def documents_es_fx():
    client = _es_client()
    yield client
    client.indices.delete(index=DOCUMENTS_INDEX + '*')
    client.indices.flush()
    IndexManager.index_cache.clear()


@pytest.fixture(scope='session')
def datatxt_fx():
    host, port = DATATXT_ENDPOINT.rsplit(':', 1)
    for sep in ('//', '@'):
        idx = host.find(sep)
        if idx > -1:
            host = host[idx + len(sep):]
    wait_service(60, is_open, [host, port])
    yield DATATXT_ENDPOINT


@pytest.fixture(scope='session')
def text_samples_fx():
    yield TEXT_SAMPLES


@pytest.fixture(scope='session')
def datatxt_response_fx():
    yield copy.deepcopy(DATATXT_RESPONSE_SAMPLE)


@pytest.fixture(scope='session')
def spots_trie_fx(samples_fx):
    yield os.path.abspath(os.path.join(samples_fx, 'sample_spots'))


@pytest.fixture(scope='module')
def grid_map_fx(samples_fx):
    wait_service(60, is_open, [DB_HOST, DB_PORT])
    grid_path = os.path.join(samples_fx, 'GRID_sample.txt')
    with GRIDPostgresClient(host=DB_HOST, port=DB_PORT, name=DB_NAME, user=DB_USER, password=DB_PASSWORD) as client:
        with GRIDFile(grid_path) as entities:
            client.create_table(DB_GRID_TABLE, overwrite=True)
            for row in entities.iterator():
                client.insert_row(DB_GRID_TABLE, row)
        grid_map = GRIDMap(client, table_name=DB_GRID_TABLE)
        yield grid_map
        client.drop_table(DB_GRID_TABLE)


@pytest.fixture(scope='session')
def searcher_fx(spots_trie_fx):
    yield Searcher.load(spots_trie_fx)


@pytest.fixture(scope='session')
def celery_enable_logging():
    return True


@pytest.fixture(scope='session')
def celery_includes():
    return [
        'inpipeline.tasks'
    ]


@pytest.fixture(scope='session')
def upload_path_fx():
    with TemporaryDirectory() as path:
        with patch('inpipeline.config.UPLOAD_PATH', path):
            yield path


@pytest.fixture(scope='session')
def app(upload_path_fx):
    from inpipeline.api.server import app
    app.debug = True
    return app


@pytest.fixture
def add_doc_req_fx(samples_fx):
    file_path = os.path.join(samples_fx, 'CombinedDocument_APPDOC_662514_64539_001.pdf')
    with NamedTemporaryFile() as f:
        copyfile(file_path, f.name)
        req = {
            'document': (open(f.name, 'rb'), 'CombinedDocument_APPDOC_662514_64539_001.pdf'),
            'security_operation': json.dumps(FAKE_SECURITY),
            'doc_id': '999',
            'id': '8457835d-9eac-4b38-a6fe-58929a712c51'
        }
        yield req


@pytest.fixture(scope='module')
def api_db_fx():
    wait_service(60, is_open, [DB_HOST, DB_PORT])
    with DocumentsPostgresClient(host=DB_HOST, port=DB_PORT, name=DB_NAME, user=DB_USER, password=DB_PASSWORD) as client:
        client.create_table(DB_DOCUMENTS_TABLE, overwrite=False, raise_if_exists=False)
        yield client
        client.drop_table(DB_DOCUMENTS_TABLE)


@pytest.fixture(scope='session')
def sectors_percolator_fx(samples_fx):
    client = _es_client()
    queries = os.path.join(resources_path, 'sector_queries.pkl')
    index = versioned(PERCOLATOR_INDEX, INDEX_VERSION)
    client.indices.delete(index=index, ignore=[400, 404])
    index_queries(ELASTICSEARCH_ENDPOINT, index, queries)
    index_sample_documents(ELASTICSEARCH_ENDPOINT, index, samples_fx)
    client.indices.flush()
    yield client
    client.indices.delete(index=PERCOLATOR_INDEX + '*')
