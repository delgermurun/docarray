from .backend import WeaviateBackendMixin
from .getsetdel import GetSetDelMixin
from .seqlike import SequenceLikeMixin

__all__ = ['WeaviateStorageMixins']


class WeaviateStorageMixins(WeaviateBackendMixin, GetSetDelMixin, SequenceLikeMixin):
    ...
