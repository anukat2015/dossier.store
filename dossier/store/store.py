'''
.. autoclass:: FCStorage
.. autoclass:: Content
.. autofunction:: content_type
.. autofunction:: feature_index
'''
from __future__ import absolute_import, division, print_function
from functools import partial
from itertools import imap, izip, repeat

from dossier.fc import FeatureCollection


class Content (object):
    '''
    This class is a thin wrapper around the data that can be stored
    in a :class:`dossier.fc.store.FCStorage` database.

    While this class treats ``data`` as completely opaque, the default
    mode of operation for :class:`dossier.fc.store.FCStorage` is to
    serialize and deserialize feature collections in ``data``.
    '''
    __slots__ = ['content_id', 'data']

    def __init__(self, content_id, data):
        self.content_id, self.data = content_id, data

    def type(self):
        return content_type(self.content_id)


def feature_index(*feature_names):
    '''
    Returns a valid index ``create`` function for the feature names
    given. This can be used with the :meth:`FCStorage.add_index` method
    to create indices on any combination of features in a feature
    collection.

    :type feature_names: list(str)
    :rtype: index creation function
    '''
    def _(trans, c):
        for fname in feature_names:
            for fval in c.data.get(fname, {}).keys():
                yield trans(fval)
    return _


class FCStorage (object):
    '''
    A feature collection database stores feature collections for content
    objects like profiles from external knowledge bases.

    Every feature collection is keyed by its ``content_id``. The value
    of a ``content_id`` is specific to the type of content represented
    by the feature collection.

    By convention, every ``content_id`` has a prefix ``{type}_`` where
    ``type`` is a string identifying the type of content represented by
    the feature collection.

    The onus is on the clients of ``FCStorage`` to construct correct
    ``content_ids``.

    N.B. This class does not have to use instances of
    :class:`dossier.fc.store.Content`. Namely, the interface used is
    simply the pair of ``content_id`` and ``data`` attributes.

    .. automethod:: __init__
    .. automethod:: get
    .. automethod:: put
    .. automethod:: delete
    .. automethod:: scan
    .. automethod:: scan_prefix
    .. automethod:: index_scan
    .. automethod:: index_scan_prefix
    .. automethod:: add_index
    .. automethod:: encode
    .. automethod:: decode
    '''
    TABLE = 'fc'
    INDEX_TABLE = 'fci'

    _kvlayer_namespace = {
        TABLE: (str,),                # content_id -> feature collection
        INDEX_TABLE: (str, str, str), # idx name, value, content_id -> NULL
    }

    @staticmethod
    def encode(content):
        '''
        Given a ``content`` object, this encodes the data into a valid
        kvlayer representation (including the identifier).

        The representation of the encoding is tied to the schema of the
        underlying table.

        If you need to change the encoding scheme, subclass
        :class:`dossier.fc.store.FCStorage` and override this method.

        :type content: :class:`dossier.fc.store.Content`
        :rtype: unspecified
        '''
        return ((content.content_id,), content.data.dumps())

    @staticmethod
    def decode(key_val):
        '''
        Given a kvlayer representation of a content object,
        this decodes it and returns its corresponding
        content object with the `data` member set to a
        :class:`dossier.fc.FeatureCollection`.

        If you need to change the encoding scheme, subclass
        :class:`dossier.fc.store.FCStorage` and override this method.

        :type key_val: unspecified
        :rtype: :class:`dossier.fc.store.Content`
        '''
        key, data = key_val
        key = FCStorage._decode_key(key)
        return Content(key, FeatureCollection.loads(data))

    @staticmethod
    def _decode_key(key):
        '''
        Given a `key` from an FC store table, this returns the
        content identifier.

        This function exists to maintain backward compatibility
        with other systems. It seems that identifiers can either
        be strings or tuples of a single value.
        '''
        if isinstance(key, tuple):
            assert len(key) == 1
            return key[0]
        elif isinstance(key, basestring):
            return key
        else:
            assert False, 'bogus vertex_id %r' % key

    def __init__(self, kvl):
        '''
        Connects to a feature collection store. This also initializes
        the underlying kvlayer namespace.

        :param kvl: kvlayer storage client
        :type kvl: :class:`kvlayer.AbstractStorage`
        '''
        self._indexes = {}
        kvl.setup_namespace(self._kvlayer_namespace)
        self.kvl = kvl

    def get(self, content_id):
        '''
        Retrieves a profile feature collection from the store.

        If the feature collection does not exist ``None`` is
        returned.

        :rtype: :class:`dossier.fc.FeatureCollection`
        '''
        rows = list(self.kvl.get(self.TABLE, (content_id,)))
        assert len(rows) < 2, 'more than one FC with the same content id'
        if len(rows) == 0 or rows[0][1] is None:
            return None
        return self.decode(rows[0])

    def put(self, *content_objs, **kwargs):
        '''
        Adds a feature collection to the store with the identifier
        ``content_id``. If a feature collection already exists with the 
        identifier ``content_id``, then it is overwritten.

        This method optionally accepts a keyword argument `indexes`,
        which by default is set to ``False``. When it is ``True``,
        it will *create* new indexes for each content object for all
        indexes defined on this store.

        :param ids_and_fcs: tuples of ``content_id`` and ``fc``
        :param str content_id: identifier for the content object represented
                               by a feature collection
        :param fc: feature collection
        :type fc: :class:`dossier.fc.FeatureCollection`
        '''
        self.kvl.put(self.TABLE, *map(lambda c: self.encode(c), content_objs))
        if kwargs.get('indexes', False):
            for idx_name in self._indexes:
                self._index_put(idx_name, *content_objs)

    def delete(self, content_id):
        '''
        Deletes the content item from the store with identifier
        ``content_id``.

        :param str content_id: identifier for the content object represented
                               by a feature collection
        '''
        self.kvl.delete(self.TABLE, (content_id,))

    def scan(self, *key_ranges, **kwargs):
        '''
        Returns a generator of content objects corresponding to the
        content identifier ranges given. `key_ranges` can be a possibly
        empty list of 2-tuples, where the first element of the tuple
        is the beginning of a range and the second element is the end
        of a range. To specify the beginning or end of the table, use
        an empty tuple `()`.

        If the list is empty, then this yields all content objects in
        the storage.

        :param key_ranges: as described in
                           :meth:`kvlayer._abstract_storage.AbstractStorage`
        :param bool only_ids: *Optional.* When set, returns only the content 
                              identifiers instead of content objects.
        :rtype: generator of :class:`dossier.fc.Content`
        '''
        only_ids = kwargs.get('only_ids', False)

        # (id, id) -> ((id,), (id,))
        tuplify = lambda v: () if v is () else tuple([v])
        key_ranges = [(tuplify(s), tuplify(e)) for s, e in key_ranges]

        scan = self.kvl.scan_keys if only_ids else self.kvl.scan
        decoder = self._decode_key if only_ids else self.decode
        return imap(decoder, scan(self.TABLE, *key_ranges))

    def scan_prefix(self, prefix, only_ids=False):
        '''
        Returns a generator of content objects with a content id
        ``prefix``.

        :param key_ranges: as described in
                           :meth:`kvlayer._abstract_storage.AbstractStorage`
        :param bool only_ids: When set, returns only the content identifiers 
                              instead of content objects.
        :rtype: generator of :class:`dossier.fc.Content`
        '''
        start, end = (prefix,), (prefix + '\xff',)
        scan = self.kvl.scan_keys if only_ids else self.kvl.scan
        decoder = self._decode_key if only_ids else self.decode
        return imap(decoder, scan(self.TABLE, (start, end)))

    def index_scan(self, idx_name, val):
        '''
        Returns a generator of content identifiers that have an entry
        in the index ``idx_name`` with value ``val`` (after index
        transforms are applied).

        If the index named by ``idx_name`` is not registered, then a
        :exc:`~exceptions.KeyError` is raised.

        :param str idx_name: name of index
        :param val: the value to use to search the index
        :rtype: generator of ``content_id``
        :raises: :exc:`~exceptions.KeyError`
        '''
        idx = self._index(idx_name)['transform']
        key = (idx_name, idx(val))
        keys = self.kvl.scan_keys(self.INDEX_TABLE, (key, key))
        return imap(lambda k: k[2], keys)

    def index_scan_prefix(self, idx_name, val_prefix):
        '''
        Returns a generator of content identifiers that have an entry
        in the index ``idx_name`` with a prefix equal to ``val`` (after
        index transforms are applied).

        If the index named by ``idx_name`` is not registered, then a
        :exc:`~exceptions.KeyError` is raised.

        :param str idx_name: name of index
        :param val: the value to use to search the index
        :rtype: generator of ``content_id``
        :raises: :exc:`~exceptions.KeyError`
        '''
        idx = self._index(idx_name)['transform']
        val_prefix = idx(val_prefix)
        s, e = (idx_name, val_prefix), (idx_name, val_prefix + '\xff')
        keys = self.kvl.scan_keys(self.INDEX_TABLE, (s, e))
        return imap(lambda k: k[2], keys)

    def add_index(self, idx_name, create, transform):
        '''
        Adds an index transform to the current FC store. Once an index
        with name ``idx_name`` is added, it will be available in all
        ``index_*`` methods. Additionally, the index will be automatically
        updated on calls to :meth:`~dossier.fc.store.FCStorage.put`.

        If an index with name ``idx_name`` already exists, then it is
        updated.

        For example, to add an index on the ``boNAME`` feature, you can
        use the ``feature_index`` helper function:

        .. code-block:: python

            fcstore.add_index('boNAME',
                              feature_index('boNAME'),
                              lambda s: s.encode('utf-8'))

        Another example for creating an index on names:

        .. code-block:: python

            fcstore.add_index('NAME',
                              feature_index('canonical_name', 'NAME'),
                              lambda s: s.lower().encode('utf-8'))

        :param idx_name: The name of the index. Must be UTF-8 encodable.
        :type idx_name: str or unicode
        :param create: A function that accepts the ``transform`` function and
                       a content object and produces a generator of
                       index values from the content object using the
                       ``transform``.
        :param transform: A function that accepts an arbitrary value and
                          applies a transform to it.
        '''
        idx_name = idx_name.encode('utf-8')
        self._indexes[idx_name] = {'create': create, 'transform': transform}

    # These methods are provided if you really need them, but hopefully
    # `put` is more convenient.

    def _index_put(self, idx_name, *content_objs):
        '''
        Adds new index values for index ``idx_name`` for the content
        objects given.
        '''
        keys = self._index_keys_for(idx_name, *content_objs)
        with_vals = imap(lambda k: (k, '0'), keys)
        self.kvl.put(self.INDEX_TABLE, *with_vals)

    def _index_put_raw(self, idx_name, content_id, val):
        '''
        Adds a new index key corresponding to
        ``(idx_name, transform(val), content_id)``.

        This method bypasses the *creation* of indexes from content
        objects, but values are still transformed.
        '''
        idx = self._index(idx_name)['transform']
        key = (idx_name, idx(val), content_id)
        self.kvl.put(self.INDEX_TABLE, (key, '0'))

    def _index_delete(self, idx_name, *content_objs):
        '''
        Deletes all index entries for the content objects given.
        '''
        keys = self._index_keys_for(idx_name, *content_objs)
        self.kvl.delete(self.INDEX_TABLE, *keys)

    def _index_keys_for(self, idx_name, *content_objs):
        '''
        Returns a generator of index keys for the ``content_objs``
        given. The index keys have the form ``(idx_name, idx_val,
        content_id)``.
        '''
        idx = self._index(idx_name)
        icreate, itrans = idx['create'], idx['transform']
        gens = izip(repeat(repeat(idx_name)),
                    imap(partial(icreate, itrans), content_objs),
                    list(imap(lambda c: repeat(c.content_id), content_objs)))
        # gens is [repeat(name), gen of index values, repeat(content_id)]
        # `name` is fixed for all values, but `content_id` is specific to each
        # content object.
        return (key for gs in gens for key in izip(*gs))

    def _index(self, name):
        try:
            return self._indexes[name]
        except KeyError:
            raise KeyError('Index "%s" has not been registered with '
                           'this FC store.' % name)


def content_type(content_id):
    '''
    Returns the type of content represented by the feature collection
    with identifier ``content_id``.

    This relies on the convention that all ``content_id``'s are prefixed
    with ``{type}_``. If a non-conforming ``content_id`` is given, then
    a :exc:`ValueError` is raised.

    :param str content_id: content identifier
    :rtype: str
    :raises: :exc:`~exceptions.ValueError`
    '''
    underscore = content_id.find('_')
    if underscore == -1:
        raise ValueError('Invalid content_id: "%s" (no type prefix)'
                         % content_id)
    return content_id[0:underscore]
