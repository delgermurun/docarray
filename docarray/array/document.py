from .memory import DocumentArrayInMemory as DocumentArray

__all__ = ['DocumentArray']

def _extend_instance(obj, cls):
    """Apply mixins to a class instance after creation"""
    base_cls = obj.__class__
    base_cls_name = obj.__class__.__name__
    obj.__class__ = type(base_cls_name, (base_cls, cls), {})


class DocumentArray(AllMixins):
    def __init__(self, *args, storage: str = 'memory', **kwargs):
        super().__init__()
        if storage == 'memory':
            from .storage.memory import MemoryStorageMixins

            _extend_instance(self, MemoryStorageMixins)
        elif storage == 'sqlite':
            from .storage.sqlite import SqliteStorageMixins

            _extend_instance(self, SqliteStorageMixins)
        elif storage == 'weaviate':
            from .storage.weaviate import WeaviateStorageMixins

            _extend_instance(self, WeaviateStorageMixins)
        else:
            raise ValueError(f'storage=`{storage}` is not supported.')

        self._init_storage(*args, **kwargs)
