from typing import Iterator, Union, Sequence, Iterable, MutableSequence

from .... import Document
from .helper import wmap


class SequenceLikeMixin(MutableSequence[Document]):
    """Implement sequence-like methods"""

    def insert(self, index: int, value: 'Document'):
        """Insert `doc` at `index`.
        :param index: Position of the insertion.
        :param value: The doc needs to be inserted.
        """
        self._offset2ids.insert(index, wmap(value.id))
        self._client.data_object.create(**self._doc2weaviate_create_payload(value))


    def __eq__(self, other):
        # two DAW are considered as the same if they have the same client meta data
        return (
            type(self) is type(other)
            and self._client.get_meta() == other._client.get_meta()
        )

    def __len__(self):
        return (
            self._client.query.aggregate(self._class_name)
            .with_meta_count()
            .do()['data']['Aggregate'][self._class_name][0]['meta']['count']
        )

    def __iter__(self) -> Iterator['Document']:
        for wid in range(len(self._offset2ids)):
            yield self[wid]

    def __contains__(self, x: Union[str, 'Document']):
        if isinstance(x, str):
            return self._client.data_object.exists(wmap(x))
        elif isinstance(x, Document):
            return self._client.data_object.exists(wmap(x.id))
        else:
            return False

    def clear(self):
        """Clear the data of :class:`DocumentArray with weaviate storage`"""
        self._client.schema.delete_all()
        self._offset2ids.clear()

    def __bool__(self):
        """To simulate ```l = []; if l: ...```
        :return: returns true if the length of the array is larger than 0
        """
        return len(self) > 0

    def __repr__(self):
        return f'<{self.__class__.__name__} (length={len(self)}) at {id(self)}>'

    def __add__(self, other: 'Document'):
        v = type(self)()
        for doc in self:
            v.append(doc)
        for doc in other:
            v.append(doc)
        return v

    def extend(self, values: Iterable['Document']) -> None:
        with self._client.batch as _b:
            for d in values:
                _b.add_data_object(**self._doc2weaviate_create_payload(d))
                self._offset2ids.append(wmap(d.id))
