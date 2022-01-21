from .mixins import AllMixins

from .storage.memory import StorageMixins as MemoryStorageMixins
from .storage.sqlite import StorageMixins as SqliteStorageMixins
from .storage.weaviate import StorageMixins as WeaviateStorageMixins

def _extend_instance(obj, cls):
    """Apply mixins to a class instance after creation"""
    base_cls = obj.__class__
    base_cls_name = obj.__class__.__name__
    obj.__class__ = type(base_cls_name, (base_cls, cls), {})


class DocumentArray(AllMixins):
    def __new__(cls, *args, storage: str = 'memory', **kwargs):
        if cls is DocumentArray:
            if storage == 'memory':
                instance = super().__new__(DocumentArrayMemory)
            elif storage == 'sqlite':
                instance = super().__new__(DocumentArraySqlite)
            elif storage == 'weaviate':
                instance = super().__new__(DocumentArrayWeaviate)
            else:
                raise ValueError(f'storage=`{storage}` is not supported.')
        else:
            instance = super().__new__(cls)
        return instance

    def __init__(self, *args, storage: str = 'memory', **kwargs):
        super().__init__()
        self._init_storage(*args, **kwargs)


class DocumentArrayMemory(DocumentArray, MemoryStorageMixins):
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)


class DocumentArraySqlite(DocumentArray, SqliteStorageMixins):
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)


class DocumentArrayWeaviate(DocumentArray, WeaviateStorageMixins):
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)
