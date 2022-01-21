import uuid
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
import scipy.sparse

from .... import Document
from ..base.backend import BaseBackendMixin

if TYPE_CHECKING:
    from ....types import (
        DocumentArraySourceType,
    )


class BackendMixin(BaseBackendMixin):
    """Provide necessary functions to enable this storage backend. """
    def _init_storage(
        self,
        docs: Optional['DocumentArraySourceType'] = None,
        client: Optional[weaviate.Client] = None,
        array_id: Optional[str] = None,
    ): 
        from ... import DocumentArray
        self._schemas = None
        self._client = client or weaviate.Client('http://localhost:8080')
        self._upload_weaviate_schema(array_id)
        self._offset2ids = self._load_offset2ids_meta()

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

    def _get_schema_by_name(self, cls_name):
        return {
            'classes': [
                {
                    'class':  cls_name,
                    'vectorIndexConfig': {
                        'skip': True # TODO setting this to true for now but may change
                    },
                    'properties': [
                        {'dataType': ['blob'], 'name': '_serialized'},
                    ],
                },
                {
                    'class':  cls_name + 'Meta',
                    'vectorIndexConfig': {
                        'skip': True
                    },
                    'properties': [
                        {'dataType': ['string[]'], 'name': '_offset2ids'},
                    ],
                }
            ]
        }

    def _upload_weaviate_schema(self, cls_name: Optional[str] = None):
        if not cls_name:
            doc_schema = self._get_schema_by_name(self._get_weaviate_class_name())
            while self._client.schema.contains(doc_schema):
                doc_schema = self._get_schema_by_name(self._get_weaviate_class_name())
        else:
            doc_schema = self._get_schema_by_name(cls_name)
        self._client.schema.create(doc_schema)
        self._schemas = doc_schema

    def _update_offset2ids_meta(self):
        # TODO: This function is like _rebuld_id2offset,
        # should not be frequently called 
        if not self._schemas:
            raise ValueError('cannot update offsetids meta since schema is missing')
        if self._client.schema.contains(self._schemas):
            self._client.schema.delete_class(self._meta_name)
        self._client.schema.create_class(self._meta_schema)
        self._client.data_object.create({'_offset2ids': self._offset2ids}, self._meta_name)

    def _load_offset2ids_meta(self):
        resp = self._client.query.get(self._meta_name, ['_offset2ids']).do().get('data', {}).get('Get', {}).get(self._meta_name, [])
        if not resp:
            return []
        elif len(resp) == 1:
            return resp[0]['_offset2ids']
        else:
            raise ValueError('received multiple meta copies which is invalid')

    @property
    def _class_name(self):
        if not self._schemas:
            return None
        return self._schemas['classes'][0]['class']
        
    @property
    def _meta_name(self):
        if not self._schemas:
            return None
        return self._schemas['classes'][1]['class']
        
    @property
    def _class_schema(self):
        if not self._schemas:
            return None
        return self._schemas['classes'][0]
        
    @property
    def _meta_schema(self):
        if not self._schemas and len(self._schemas) < 2:
            return None
        return self._schemas['classes'][1]
        
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
             uuid=self.wmap(value.id),
             vector=embedding,
         )
    
    def wmap(self, doc_id):
        # appending class name to doc id to handle the case:
        # daw1 = DocumentArrayWeaviate([Document(id=str(i), text='hi') for i in range(3)])
        # daw2 = DocumentArrayWeaviate([Document(id=str(i), text='bye') for i in range(3)])
        # daw2[0, 'text'] == 'hi' # this will be False if we don't append class name
        return str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id+self._class_name))
