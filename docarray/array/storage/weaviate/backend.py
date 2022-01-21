import itertools
from typing import (
    Generator,
    Iterator,
    Dict,
    Sequence,
    Optional,
    TYPE_CHECKING,
)

import weaviate
import uuid

from .... import Document
from ..base.backend import BaseBackendMixin
from .helper import wmap

if TYPE_CHECKING:
    from ....types import (
        DocumentArraySourceType,
    )


class BackendMixin(BaseBackendMixin):
    """Provide necessary functions to enable this storage backend. """
    def _init_storage(
        self,
        docs: Optional['DocumentArraySourceType'] = None,
        array_id: str = None,
    ): 
        from ... import DocumentArray
        self._client = weaviate.Client('http://localhost:8080')
        if array_id:
            self._class_name = array_id
        else:
            self._class_name = self._get_weaviate_class_name()
            self._upload_weaviate_schema()
        self._offset2ids = []
        if docs is None:
            return
        elif isinstance(
            docs, (DocumentArray, Sequence, Generator, Iterator, itertools.chain)
        ):
            self.extend(Document(d, copy=True) for d in docs)
        else:
            if isinstance(docs, Document):
                self.append(docs)

    def _get_weaviate_class_name(self):
        return ''.join([i for i in uuid.uuid1().hex if not i.isdigit()]).capitalize()

    def _upload_weaviate_schema(self):
        doc_schema = {
            'classes': [
                {
                    'class': self._class_name,
                    'properties': [
                        {'dataType': ['blob'], 'name': '_serialized'},
                    ],
                }
            ]
        }
        self._client.schema.delete_all()
        self._client.schema.create(doc_schema)

    def _doc2weaviate_create_payload(self, value: 'Document'):
         if value.embedding is None:
             embedding = [0]
         elif isinstance(value.embedding, scipy.sparse.spmatrix):
             embedding = value.embedding.toarray()
         else:
             embedding = value.embedding

         return dict(
             data_object={'_serialized': value.to_base64()},
             class_name=self._class_name,
             uuid=wmap(value.id),
             vector=embedding,
         )

