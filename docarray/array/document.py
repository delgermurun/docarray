import sys
from .mixins import AllMixins

__all__ = ['DocumentArray']

def _extend_instance(obj, cls):
    """Apply mixins to a class instance after creation"""
    base_cls = obj.__class__
    base_cls_name = obj.__class__.__name__
    obj.__class__ = type(base_cls_name, (base_cls, cls), {})


class DocumentArray(AllMixins):
    def __new__(cls, *args, storage: str = 'memory', **kwargs):
        # TODO: The following is to workaround the pickling issue
        # Will remove once this is rebased on latest branch
        mixin = None
        if storage == 'memory':
            from .storage.memory import MemoryStorageMixins
            mixin = MemoryStorageMixins
        elif storage == 'sqlite':
            from .storage.sqlite import SqliteStorageMixins
            mixin = SqliteStorageMixins
        elif storage == 'weaviate':
            from .storage.weaviate import WeaviateStorageMixins
            mixin = WeaviateStorageMixins
        else:
            raise ValueError(f'storage=`{storage}` is not supported.')

        class _D(cls, mixin):
            ...

        name = cls.__name__ + storage.capitalize()
        _D.__name__ = name
        _D.__qualname__ = name
        if not hasattr(sys.modules[__name__], name):
            setattr(sys.modules[__name__], name, _D)
        return object.__new__(getattr(sys.modules[__name__], name))

    def __init__(self, *args, storage: str = 'memory', **kwargs):
        super().__init__()
        self._init_storage(*args, **kwargs)
