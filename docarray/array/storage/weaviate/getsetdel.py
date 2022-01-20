import itertools
from collections.abc import Iterable as _Iterable
from typing import (
    Sequence,
    Iterable,
    Any,
)

from ..base.getsetdel import BaseGetSetDelMixin
from .... import Document, DocumentArray
from .helper import wmap


class GetSetDelMixin(BaseGetSetDelMixin):
    def _getitem(self, wid: str):
        resp = self._client.data_object.get_by_id(wid, with_vector=True)
        if not resp:
            raise KeyError(wid)
        return Document.from_base64(resp['properties']['_serialized'])

    def _setitem(self, wid: str, value: Document):
        if self._client.data_object.exists(wid):
            self._client.data_object.delete(wid)
        self._client.data_object.create(**self._doc2weaviate_create_payload(value))
        self._offset2ids[self._offset2ids.index(wid)] = wmap(value.id)

    def _delitem(self, wid: str):
        self._offset2ids.pop(self._offset2ids.index(wid))
        self._client.data_object.delete(wid)

    def _get_doc_by_offset(self, offset: int) -> 'Document':
        return self._getitem(self._offset2ids[offset])

    def _get_doc_by_id(self, _id: str) -> 'Document':
        return self._getitem(wmap(_id))

    def _get_docs_by_slice(self, _slice: slice) -> Iterable['Document']:
        wids = self._offset2ids[_slice]
        return (self._getitem(wid) for wid in wids)

    def _set_doc_by_offset(self, offset: int, value: 'Document'):
        wid = self._offset2ids[offset]
        self._setitem(wid, value)
        # update weaviate id
        self._offset2ids[offset] = wmap(value.id)

    def _set_doc_value_pairs(self, docs: Iterable['Document'], values: Iterable['Document']):
        # TODO: update/optimize this method
        map_doc_id_to_offset = {doc.id: offset for offset, doc in enumerate(docs)}
        new = DocumentArray([d for d in self])
        for d in new.flatten():
            if d.id not in map_doc_id_to_offset:
                continue
            v = values[map_doc_id_to_offset[d.id]]
            d._data = v._data

        for _d, _v in zip(self, new):
            self._setitem(wmap(_d.id), _v)

    def _set_doc_by_id(self, _id: str, value: 'Document'):
        self._setitem(wmap(_id), value)

    def _set_docs_by_slice(self, _slice: slice, value: Sequence['Document']):
        wids = self._offset2ids[_slice]
        if not isinstance(value, _Iterable):
            raise TypeError('can only assign an iterable')
        for _i, _v in zip(wids, value):
            self._setitem(_i, _v)

    def _set_doc_attr_by_id(self, _id: str, attr: str, value: Any):
        if attr == 'id' and value is None:
            raise ValueError('cannot pop id from Document stored with weaviate')
        doc = self[_id]
        setattr(doc, attr, value)
        self._setitem(wmap(doc.id), doc)

    def _del_doc_by_offset(self, offset: int):
        self._delitem(self._offset2ids[offset])

    def _del_doc_by_id(self, _id: str):
        self._delitem(wmap(_id))

    def _del_docs_by_slice(self, _slice: slice):
        start = _slice.start or 0
        stop = _slice.stop or len(self)
        step = _slice.step or 1
        del self[list(range(start, stop, step))]

    def _del_all_docs(self):
        self._client.schema.delete_all()
        self._offset2ids.clear()
        self._upload_weaviate_schema()

    def _del_docs_by_mask(self, mask: Sequence[bool]):
        idxs = list(
            itertools.compress(self._offset2ids, (not _i for _i in mask))
        )
        for _idx in reversed(idxs):
            self._delitem(_idx)
