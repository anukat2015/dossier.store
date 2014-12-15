'''dossier.store.Store

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
from collections import OrderedDict
from itertools import imap
import logging
from operator import itemgetter
import struct

import snappy

from dossier.fc import FeatureCollection


logger = logging.getLogger(__name__)


def feature_index(*feature_names):
    '''Returns a index creation function.

    Returns a valid index ``create`` function for the feature names
    given. This can be used with the :meth:`Store.define_index`
    method to create indexes on any combination of features in a
    feature collection.

    :type feature_names: list(unicode)
    :rtype: ``(val -> index val)
              -> (content_id, FeatureCollection)
              -> generator of [index val]``
    '''
    def _(trans, (cid, fc)):
        for fname in feature_names:
            feat = fc.get(fname)
            if feat is None:
                continue
            elif isinstance(feat, unicode):
                yield trans(feat)
            else:  # string counter, sparse/dense vector
                for val in feat.iterkeys():
                    yield trans(val)
    return _


def basic_transform(val):
    '''A basic transform for strings and integers.'''
    if isinstance(val, int):
        return struct.pack('>i', val)
    else:
        return safe_lower_utf8(val)


def safe_lower_utf8(x):
    '''x.lower().encode('utf-8') where x can be None, str, or unicode'''
    if x is None:
        return None
    x = x.lower()
    if isinstance(x, unicode):
        return x.encode('utf-8')
    return x


class Store(object):
    '''A feature collection database.

    A feature collection database stores feature collections for content
    objects like profiles from external knowledge bases.

    Every feature collection is keyed by its ``content_id``, which is a
    byte string. The value of a ``content_id`` is specific to the type
    of content represented by the feature collection. In other words,
    its representation is unspecified.

    .. automethod:: __init__
    .. automethod:: get
    .. automethod:: get_many
    .. automethod:: put
    .. automethod:: delete
    .. automethod:: delete_all
    .. automethod:: scan
    .. automethod:: scan_ids
    .. automethod:: scan_prefix
    .. automethod:: scan_prefix_ids

    **Methods for indexing:**

    .. automethod:: index_scan
    .. automethod:: index_scan_prefix
    .. automethod:: index_scan_prefix_and_return_key
    .. automethod:: define_index
    '''
    config_name = 'dossier.store'
    TABLE = 'fc'
    INDEX_TABLE = 'fci'

    _kvlayer_namespace = {
        TABLE: (str,),                # content_id -> feature collection
        INDEX_TABLE: (str, str, str), # idx name, value, content_id -> NUL
    }

    def __new__(cls, kvlclient, impl=None, feature_indexes=None):
        if impl is None:
            return super(Store, cls).__new__(cls, kvlclient,
                                             feature_indexes=feature_indexes)
        else:
            mod, cls_name = impl.split(':')
            cls = getattr(__import__(mod, fromlist=[cls_name]), cls_name)
            return cls.__new__(cls, kvlclient, feature_indexes=feature_indexes)

    def __init__(self, kvlclient, impl=None, feature_indexes=None):
        '''Connects to a feature collection store.

        This also initializes the underlying kvlayer namespace.

        :param kvl: kvlayer storage client
        :type kvl: :class:`kvlayer.AbstractStorage`
        :rtype: :class:`Store`
        '''
        self._indexes = OrderedDict()
        kvlclient.setup_namespace(self._kvlayer_namespace)
        self.kvl = kvlclient
        for name in feature_indexes or []:
            self.define_index(name, feature_index(name), basic_transform)

    def get(self, content_id):
        '''Retrieve a feature collection from the store.  This is the same as
        get_many([content_id])

        If the feature collection does not exist ``None`` is
        returned.

        :type content_id: str
        :rtype: :class:`dossier.fc.FeatureCollection`

        '''
        rows = list(self.kvl.get(self.TABLE, (content_id,)))
        assert len(rows) < 2, 'more than one FC with the same content id'
        if len(rows) == 0 or rows[0][1] is None:
            return None
        return fc_loads(rows[0][1])

    def get_many(self, content_id_list):
        '''Yield (content_id, data) tuples for ids in list.

        As with :meth:`get`, if a content_id in the list is missing,
        then it is yielded with a data value of `None`.

        :type content_id_list: list<str>
        :rtype: yields tuple(str, :class:`dossier.fc.FeatureCollection`)

        '''
        content_id_keys = [tuplify(x) for x in content_id_list]
        for row in self.kvl.get(self.TABLE, *content_id_keys):
            content_id = row[0][0]
            data = row[1]
            if data is not None:
                data = fc_loads(data)
            yield (content_id, data)

    def put(self, items, indexes=True):
        '''Add feature collections to the store.

        Given an iterable of tuples of the form
        ``(content_id, feature collection)``, add each to the store
        and overwrite any that already exist.

        This method optionally accepts a keyword argument `indexes`,
        which by default is set to ``True``. When it is ``True``,
        it will *create* new indexes for each content object for all
        indexes defined on this store.

        Note that this will not update existing indexes. (There is
        currently no way to do this without running some sort of
        garbage collection process.)

        :param iterable items: iterable of
                               ``(content_id, FeatureCollection)``.
        :type fc: :class:`dossier.fc.FeatureCollection`
        '''
        # So why accept an iterable? Ideally, some day, `kvlayer.put` would
        # accept an iterable, so we should too.
        #
        # But we have to transform it to a list in order to update indexes
        # anyway. Well, if we don't have to update indexes, then we can avoid
        # loading everything into memory, which seems like an optimization
        # worth having even if it's only some of the time.
        #
        # N.B. If you're thinking, "Just use itertools.tee", then you should
        # heed this warning from Python docs: "This itertool may require
        # significant auxiliary storage (depending on how much temporary data
        # needs to be stored). In general, if one iterator uses most or all of
        # the data before another iterator starts, it is faster to use list()
        # instead of tee()."
        #
        # i.e., `tee` has to store everything into memory because `kvlayer`
        # will exhaust the first iterator before indexes get updated.
        items = list(items)
        self.kvl.put(self.TABLE,
                     *imap(lambda (cid, fc): ((cid,), fc_dumps(fc)), items))
        if indexes:
            for idx_name in self._indexes:
                self._index_put(idx_name, *items)

    def delete(self, content_id):
        '''Delete a feature collection from the store.

        Deletes the content item from the store with identifier
        ``content_id``.

        :param str content_id: identifier for the content object
                               represented by a feature collection
        '''
        self.kvl.delete(self.TABLE, (content_id,))

    def delete_all(self):
        '''Deletes all storage.

        This includes every content object and all index data.
        '''
        self.kvl.clear_table(self.TABLE)
        self.kvl.clear_table(self.INDEX_TABLE)

    def scan(self, *key_ranges):
        '''Retrieve feature collections in a range of ids.

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
        :rtype: generator of
                (``content_id``, :class:`dossier.fc.FeatureCollection`).
        '''
        # (id, id) -> ((id,), (id,))
        key_ranges = [(tuplify(s), tuplify(e)) for s, e in key_ranges]
        return imap(lambda (cid, fc): (cid[0], fc_loads(fc)),
                    self.kvl.scan(self.TABLE, *key_ranges))

    def scan_ids(self, *key_ranges):
        '''Retrieve content ids in a range of ids.

        Returns a generator of ``content_id`` corresponding to the
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
        key_ranges = [(tuplify(s), tuplify(e)) for s, e in key_ranges]
        scanner = self.kvl.scan_keys(self.TABLE, *key_ranges)
        return imap(itemgetter(0), scanner)

    def scan_prefix(self, prefix):
        '''Returns a generator of content objects matching a prefix.

        The ``prefix`` here is a prefix for ``content_id``.

        :type prefix: str
        :rtype: generator of
                (``content_id``, :class:`dossier.fc.FeatureCollection`).
        '''
        return self.scan((prefix, prefix + '\xff'))

    def scan_prefix_ids(self, prefix):
        '''Returns a generator of content ids matching a prefix.

        The ``prefix`` here is a prefix for ``content_id``.

        :type prefix: str
        :rtype: generator of ``content_id``
        '''
        return self.scan_ids((prefix, prefix + '\xff'))

    def index_scan(self, idx_name, val):
        '''Returns ids that match an indexed value.

        Returns a generator of content identifiers that have an entry
        in the index ``idx_name`` with value ``val`` (after index
        transforms are applied).

        If the index named by ``idx_name`` is not registered, then a
        :exc:`~exceptions.KeyError` is raised.

        :param unicode idx_name: name of index
        :param val: the value to use to search the index
        :type val: unspecified (depends on the index, usually ``unicode``)
        :rtype: generator of ``content_id``
        :raises: :exc:`~exceptions.KeyError`
        '''
        idx = self._index(idx_name)['transform']
        key = (idx_name.encode('utf-8'), idx(val))
        keys = self.kvl.scan_keys(self.INDEX_TABLE, (key, key))
        return imap(lambda k: k[2], keys)

    def index_scan_prefix(self, idx_name, val_prefix):
        '''Returns ids that match a prefix of an indexed value.

        Returns a generator of content identifiers that have an entry
        in the index ``idx_name`` with prefix ``val_prefix`` (after
        index transforms are applied).

        If the index named by ``idx_name`` is not registered, then a
        :exc:`~exceptions.KeyError` is raised.

        :param unicode idx_name: name of index
        :param val_prefix: the value to use to search the index
        :type val: unspecified (depends on the index, usually ``unicode``)
        :rtype: generator of ``content_id``
        :raises: :exc:`~exceptions.KeyError`
        '''
        return self._index_scan_prefix_impl(
            idx_name, val_prefix, lambda k: k[2])

    def index_scan_prefix_and_return_key(self, idx_name, val_prefix):
        '''Returns ids that match a prefix of an indexed value, and the
        specific key that matched the search prefix.

        Returns a generator of (index key, content identifier) that
        have an entry in the index ``idx_name`` with prefix
        ``val_prefix`` (after index transforms are applied).

        If the index named by ``idx_name`` is not registered, then a
        :exc:`~exceptions.KeyError` is raised.

        :param unicode idx_name: name of index
        :param val_prefix: the value to use to search the index
        :type val: unspecified (depends on the index, usually ``unicode``)
        :rtype: generator of (``index key``, ``content_id``)
        :raises: :exc:`~exceptions.KeyError`

        '''
        return self._index_scan_prefix_impl(
            idx_name, val_prefix, lambda k: (k[1], k[2]))

    def _index_scan_prefix_impl(self, idx_name, val_prefix, retfunc):
        '''Implementation for index_scan_prefix and
        index_scan_prefix_and_return_key, parameterized on return
        value function.

        retfunc gets passed a key tuple from the index:
        (index name, index value, content_id)
        '''
        idx = self._index(idx_name)['transform']
        val_prefix = idx(val_prefix)

        idx_name = idx_name.encode('utf-8')
        s = (idx_name, val_prefix)
        e = (idx_name, val_prefix + '\xff')
        keys = self.kvl.scan_keys(self.INDEX_TABLE, (s, e))
        return imap(retfunc, keys)

    def define_index(self, idx_name, create, transform):
        '''Add an index to this store instance.

        Adds an index transform to the current FC store. Once an index
        with name ``idx_name`` is added, it will be available in all
        ``index_*`` methods. Additionally, the index will be automatically
        updated on calls to :meth:`~dossier.fc.store.Store.put`.

        If an index with name ``idx_name`` already exists, then it is
        overwritten.

        Note that indexes do *not* persist. They must be re-defined for
        each instance of :class:`Store`.

        For example, to add an index on the ``boNAME`` feature, you can
        use the ``feature_index`` helper function:

        .. code-block:: python

            store.define_index('boNAME',
                               feature_index('boNAME'),
                               lambda s: s.encode('utf-8'))

        Another example for creating an index on names:

        .. code-block:: python

            store.define_index('NAME',
                               feature_index('canonical_name', 'NAME'),
                               lambda s: s.lower().encode('utf-8'))

        :param idx_name: The name of the index. Must be UTF-8 encodable.
        :type idx_name: unicode
        :param create: A function that accepts the ``transform`` function and
                       a pair of ``(content_id, fc)`` and produces a generator
                       of index values from the pair given using ``transform``.
        :param transform: A function that accepts an arbitrary value and
                          applies a transform to it. This transforms the
                          *stored* value to the *index* value. This *must*
                          produce a value with type `str` (or `bytes`).
        '''
        assert isinstance(idx_name, (str, unicode))
        idx_name = idx_name.decode('utf-8')
        self._indexes[idx_name] = {'create': create, 'transform': transform}

    def index_names(self):
        '''Returns a list of all defined index names.

        :rtype: list of unicode
        '''
        return self._indexes.keys()

    # These methods are provided if you really need them, but hopefully
    # `put` is more convenient.

    def _index_put(self, idx_name, *ids_and_fcs):
        '''Add new index values.

        Adds new index values for index ``idx_name`` for the pairs
        given. Each pair should be a content identifier and a
        :class:`dossier.fc.FeatureCollection`.

        :type idx_name: unicode
        :type ids_and_fcs: ``[(content_id, FeatureCollection)]``
        '''
        keys = self._index_keys_for(idx_name, *ids_and_fcs)
        with_vals = map(lambda k: (k, '0'), keys)
        # TODO: use imap when kvl.put takes an iterable
        self.kvl.put(self.INDEX_TABLE, *with_vals)

    def _index_put_raw(self, idx_name, content_id, val):
        '''Add new raw index values.

        Adds a new index key corresponding to
        ``(idx_name, transform(val), content_id)``.

        This method bypasses the *creation* of indexes from content
        objects, but values are still transformed.

        :type idx_name: unicode
        :type content_id: str
        :type val: unspecified (depends on the index, usually ``unicode``)
        '''
        idx = self._index(idx_name)['transform']
        key = (idx_name.encode('utf-8'), idx(val), content_id)
        self.kvl.put(self.INDEX_TABLE, (key, '0'))

    def _index_keys_for(self, idx_name, *ids_and_fcs):
        '''Returns a generator of index triples.

        Returns a generator of index keys for the ``ids_and_fcs`` pairs
        given. The index keys have the form ``(idx_name, idx_val,
        content_id)``.

        :type idx_name: unicode
        :type ids_and_fcs: ``[(content_id, FeatureCollection)]``
        :rtype: generator of ``(str, str, str)``
        '''
        idx = self._index(idx_name)
        icreate, itrans = idx['create'], idx['transform']
        if isinstance(idx_name, unicode):
            idx_name = idx_name.encode('utf-8')
        for cid_fc in ids_and_fcs:
            content_id = cid_fc[0]
            for index_value in icreate(itrans, cid_fc):
                if index_value:
                    yield (idx_name, index_value, content_id)

    def _index(self, name):
        '''Returns index transforms for ``name``.

        :type name: unicode
        :rtype: ``{ create |--> function, transform |--> function }``
        '''
        name = name.decode('utf-8')
        try:
            return self._indexes[name]
        except KeyError:
            raise KeyError('Index "%s" has not been registered with '
                           'this FC store.' % name)


def tuplify(v):
    if v is None:
        return None
    if isinstance(v, tuple):
        return v
    return (v,)


def fc_loads(raw):
    return FeatureCollection.loads(snappy.decompress(raw))


def fc_dumps(fc):
    return snappy.compress(fc.dumps())
