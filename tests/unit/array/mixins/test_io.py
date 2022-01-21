import os
import uuid

import numpy as np
import pytest

from docarray.array.document import DocumentArrayMemory, DocumentArrayWeaviate
from tests import random_docs


@pytest.fixture
def docs():
    return random_docs(100)


@pytest.mark.slow
@pytest.mark.parametrize('method', ['json', 'binary'])
@pytest.mark.parametrize('da_cls', [DocumentArrayWeaviate])
def test_document_save_load(docs, method, tmp_path, da_cls, start_weaviate):
    tmp_file = os.path.join(tmp_path, 'test')
    da = da_cls(docs)
    da.save(tmp_file, file_format=method)
    da_r = type(da).load(tmp_file, file_format=method)

    assert type(da) is type(da_r)
    assert len(da) == len(da_r)
    for d, d_r in zip(da, da_r):
        assert d.id == d_r.id
        np.testing.assert_equal(d.embedding, d_r.embedding)
        assert d.content == d_r.content


@pytest.mark.parametrize('flatten_tags', [True, False])
@pytest.mark.parametrize('da_cls', [DocumentArrayMemory, DocumentArrayWeaviate])
def test_da_csv_write(docs, flatten_tags, tmp_path, da_cls, start_weaviate):
    tmpfile = os.path.join(tmp_path, 'test.csv')
    da = da_cls(docs)
    da.save_csv(tmpfile, flatten_tags)
    with open(tmpfile) as fp:
        assert len([v for v in fp]) == len(da) + 1


@pytest.mark.parametrize('da', [DocumentArrayMemory, DocumentArrayWeaviate])
def test_from_ndarray(da, start_weaviate):
    _da = da.from_ndarray(np.random.random([10, 256]))
    assert len(_da) == 10


@pytest.mark.parametrize('da', [DocumentArrayMemory, DocumentArrayWeaviate])
def test_from_files(da, start_weaviate):
    assert len(da.from_files(patterns='*.*', to_dataturi=True, size=1)) == 1


cur_dir = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.parametrize('da', [DocumentArrayMemory, DocumentArrayWeaviate])
def test_from_ndjson(da, start_weaviate):
    with open(os.path.join(cur_dir, 'docs.jsonlines')) as fp:
        _da = da.from_ndjson(fp)
        assert len(_da) == 2


# TODO: add weaviate support for this test
@pytest.mark.parametrize('da_cls', [DocumentArrayMemory])
def test_from_to_pd_dataframe(da_cls):
    # simple
    assert len(da_cls.from_dataframe(da_cls.empty(2).to_dataframe())) == 2

    # more complicated
    da = da_cls.empty(2)
    da.embeddings = [[1, 2, 3], [4, 5, 6]]
    da.tensors = [[1, 2], [2, 1]]
    da[0].tags = {'hello': 'world'}
    da2 = da_cls.from_dataframe(da.to_dataframe())
    assert da2[0].tags == {'hello': 'world'}
    assert da2[1].tags == {}


# TODO: add weaviate support for this test
@pytest.mark.parametrize('da_cls', [DocumentArrayMemory])
def test_from_to_bytes(da_cls):
    # simple
    assert len(da_cls.load_binary(bytes(da_cls.empty(2)))) == 2

    # more complicated
    da = da_cls.empty(2)
    da.embeddings = [[1, 2, 3], [4, 5, 6]]
    da.tensors = [[1, 2], [2, 1]]
    da[0].tags = {'hello': 'world'}
    da2 = da_cls.load_binary(bytes(da))
    assert da2.tensors == [[1, 2], [2, 1]]
    assert da2.embeddings == [[1, 2, 3], [4, 5, 6]]
    assert da2[0].tags == {'hello': 'world'}
    assert da2[1].tags == {}


# TODO: add weaviate support for this test
@pytest.mark.parametrize('da_cls', [DocumentArrayMemory])
@pytest.mark.parametrize('show_progress', [True, False])
def test_push_pull_io(da_cls, show_progress):
    da1 = da_cls.empty(10)
    da1.embeddings = np.random.random([len(da1), 256])
    random_texts = [str(uuid.uuid1()) for _ in da1]
    da1.texts = random_texts

    da1.push('myda', show_progress=show_progress)

    da2 = da_cls.pull('myda', show_progress=show_progress)

    assert len(da1) == len(da2) == 10
    assert da1.texts == da2.texts == random_texts


# TODO: add weaviate support for this test
@pytest.mark.parametrize('protocol', ['protobuf', 'pickle'])
@pytest.mark.parametrize('compress', ['lz4', 'bz2', 'lzma', 'zlib', 'gzip', None])
@pytest.mark.parametrize('da_cls', [DocumentArrayMemory])
def test_from_to_base64(protocol, compress, da_cls):
    da = da_cls.empty(10)
    da.embeddings = [[1, 2, 3]] * len(da)
    da_r = da_cls.from_base64(
        da.to_base64(protocol, compress), protocol, compress
    )
    assert da_r == da
    assert da_r[0].embedding == [1, 2, 3]
