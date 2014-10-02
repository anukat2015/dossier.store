'''dossier.store

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. autoclass:: Store
.. autofunction:: feature_index
'''
from __future__ import absolute_import, division, print_function
from functools import partial
from itertools import imap, izip, repeat
from operator import itemgetter

from dossier.fc import FeatureCollection


def feature_index(*feature_names):
    '''
    Returns a valid index ``create`` function for the feature names
    given. This can be used with the :meth:`Storage.define_index` method
    to create indices on any combination of features in a feature
    collection.

    :type feature_names: list(unicode)
    :rtype: index creation function
    '''
    def _(trans, (cid, fc)):
        for fname in feature_names:
            for fval in fc.get(fname, {}).keys():
                yield trans(fval)
    return _


class Store(object):
    '''
    A feature collection database stores feature collections for content
    objects like profiles from external knowledge bases.

    Every feature collection is keyed by its ``content_id``. The value
    of a ``content_id`` is specific to the type of content represented
    by the feature collection.

    By convention, every ``content_id`` has a prefix ``{type}_`` where
    ``type`` is a string identifying the type of content represented by
    the feature collection.

    The onus is on the clients of ``Store`` to construct correct
    ``content_ids``.

    N.B. This class does not have to use instances of
    :class:`dossier.fc.store.Content`. Namely, the interface used is
    simply the pair of ``content_id`` and ``data`` attributes.

    .. automethod:: __init__
    .. automethod:: get
    .. automethod:: put
    .. automethod:: delete
    .. automethod:: scan
    .. automethod:: scan_ids
    .. automethod:: scan_prefix
    .. automethod:: scan_prefix_ids
    .. automethod:: index_scan
    .. automethod:: index_scan_prefix
    .. automethod:: define_index
    '''
    TABLE = 'fc'
    INDEX_TABLE = 'fci'

    _kvlayer_namespace = {
        TABLE: (str,),                # content_id -> feature collection
        INDEX_TABLE: (str, str, str), # idx name, value, content_id -> NUL
    }

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
        Retrieves a feature collection from the store.

        If the feature collection does not exist ``None`` is
        returned.

        :rtype: :class:`dossier.fc.FeatureCollection`
        '''
        rows = list(self.kvl.get(self.TABLE, (content_id,)))
        assert len(rows) < 2, 'more than one FC with the same content id'
        if len(rows) == 0 or rows[0][1] is None:
            return None
        return FeatureCollection.loads(rows[0][1])

    def put(self, content_id, fc, indexes=True):
        '''
        Adds a feature collection to the store with the identifier
        ``content_id``. If a feature collection already exists with the 
        identifier ``content_id``, then it is overwritten.

        This method optionally accepts a keyword argument `indexes`,
        which by default is set to ``True``. When it is ``True``,
        it will *create* new indexes for each content object for all
        indexes defined on this store.

        :param str content_id: identifier for the content object represented
                               by a feature collection
        :param fc: feature collection
        :type fc: :class:`dossier.fc.FeatureCollection`
        '''
        self.kvl.put(self.TABLE, ((content_id,), fc.dumps()))
        if indexes:
            for idx_name in self._indexes:
                self._index_put(idx_name, (content_id, fc))

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
        :rtype: generator of :class:`dossier.fc.Content`
        '''
        # (id, id) -> ((id,), (id,))
        tuplify = lambda v: () if v is () else tuple([v])
        key_ranges = [(tuplify(s), tuplify(e)) for s, e in key_ranges]
        return self.kvl.scan(self.TABLE, *key_ranges)

    def scan_ids(self, *key_ranges, **kwargs):
        '''
        Returns a generator of content ids corresponding to the
        content identifier ranges given. `key_ranges` can be a possibly
        empty list of 2-tuples, where the first element of the tuple
        is the beginning of a range and the second element is the end
        of a range. To specify the beginning or end of the table, use
        an empty tuple `()`.

        If the list is empty, then this yields all content ids in
        the storage.

        :param key_ranges: as described in
                           :meth:`kvlayer._abstract_storage.AbstractStorage`
        :rtype: generator of ``content_id``
        '''
        # (id, id) -> ((id,), (id,))
        tuplify = lambda v: () if v is () else tuple([v])
        key_ranges = [(tuplify(s), tuplify(e)) for s, e in key_ranges]
        scanner = self.kvl.scan_keys(self.TABLE, *key_ranges)
        return imap(itemgetter(0), scanner)

    def scan_prefix(self, prefix):
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
        return self.kvl.scan(self.TABLE, (start, end))

    def scan_prefix_ids(self, prefix):
        '''
        Returns a generator of content ids that have ``prefix``.

        :param key_ranges: as described in
                           :meth:`kvlayer._abstract_storage.AbstractStorage`
        :rtype: generator of ``content_id``
        '''
        start, end = (prefix,), (prefix + '\xff',)
        scanner = self.kvl.scan_keys(self.TABLE, (start, end))
        return imap(itemgetter(0), scanner)

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
        key = (idx_name.encode('utf-8'), idx(val))
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
        s = (idx_name.encode('utf-8'), val_prefix)
        e = (idx_name.encode('utf-8'), val_prefix + '\xff')
        keys = self.kvl.scan_keys(self.INDEX_TABLE, (s, e))
        return imap(lambda k: k[2], keys)

    def define_index(self, idx_name, create, transform):
        '''
        Adds an index transform to the current FC store. Once an index
        with name ``idx_name`` is added, it will be available in all
        ``index_*`` methods. Additionally, the index will be automatically
        updated on calls to :meth:`~dossier.fc.store.Store.put`.

        If an index with name ``idx_name`` already exists, then it is
        overwritten.

        For example, to add an index on the ``boNAME`` feature, you can
        use the ``feature_index`` helper function:

        .. code-block:: python

            fcstore.define_index('boNAME',
                                 feature_index('boNAME'),
                                 lambda s: s.encode('utf-8'))

        Another example for creating an index on names:

        .. code-block:: python

            fcstore.define_index('NAME',
                                 feature_index('canonical_name', 'NAME'),
                                 lambda s: s.lower().encode('utf-8'))

        :param idx_name: The name of the index. Must be UTF-8 encodable.
        :type idx_name: unicode
        :param create: A function that accepts the ``transform`` function and
                       a pair of ``(content_id, fc)`` and produces a generator
                       of index values from the pair given using ``transform``.
        :param transform: A function that accepts an arbitrary value and
                          applies a transform to it. This transforms the
                          *stored* value to the *index* value.
        '''
        assert isinstance(idx_name, unicode)
        self._indexes[idx_name] = {'create': create, 'transform': transform}

    # These methods are provided if you really need them, but hopefully
    # `put` is more convenient.

    def _index_put(self, idx_name, *ids_and_fcs):
        '''
        Adds new index values for index ``idx_name`` for the pairs
        given. Each pair should be a content identifier and a
        feature collection.
        '''
        keys = self._index_keys_for(idx_name, *ids_and_fcs)
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
        key = (idx_name.encode('utf-8'), idx(val), content_id)
        self.kvl.put(self.INDEX_TABLE, (key, '0'))

    def _index_delete(self, idx_name, *ids_and_fcs):
        '''
        Deletes all index entries for the pairs given, where each pair
        should be a content identifier and a feature collection.
        '''
        keys = self._index_keys_for(idx_name, *ids_and_fcs)
        self.kvl.delete(self.INDEX_TABLE, *keys)

    def _index_keys_for(self, idx_name, *ids_and_fcs):
        '''
        Returns a generator of index keys for the ``ids_and_fcs`` pairs
        given. The index keys have the form ``(idx_name, idx_val,
        content_id)``.
        '''
        idx = self._index(idx_name)
        icreate, itrans = idx['create'], idx['transform']
        gens = izip(repeat(repeat(idx_name.encode('utf-8'))),
                    imap(partial(icreate, itrans), ids_and_fcs),
                    list(imap(lambda (cid, _): repeat(cid), ids_and_fcs)))
        # gens is [repeat(name), gen of index values, repeat(content_id)]
        # `name` is fixed for all values, but `content_id` is specific to each
        # content object.
        return (key for gs in gens for key in izip(*gs))

    def _index(self, name):
        assert isinstance(name, unicode)
        try:
            return self._indexes[name]
        except KeyError:
            raise KeyError('Index "%s" has not been registered with '
                           'this FC store.' % name)
