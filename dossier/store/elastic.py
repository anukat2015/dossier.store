'''A native ElasticSearch implementation for dossier.store.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import base64
from collections import OrderedDict, Mapping, defaultdict
import logging
import uuid

import cbor
from dossier.fc import FeatureCollection as FC
import yakonfig

from elasticsearch import Elasticsearch, NotFoundError, TransportError
from elasticsearch.helpers import bulk, scan


logger = logging.getLogger(__name__)


class ElasticStore(object):
    '''A feature collection store on ElasticSearch.

    Feature collections are maps from feature names to features.
    The representation of each feature is unspecified by this
    interface.

    This class exposes a similar interface to the regular ``Store``
    class, with a few additions:

      1. Canopy scans are implemented natively with ElasticSearch,
         so they are provided as methods here.
      2. On all retrieval methods, the caller can pass a list of
         feature names (or feature name wildcards) to retrieve.
         If your FCs have lots of features, this is useful when
         you only need to retrieve a small fraction of them.

    .. automethod:: __init__
    .. automethod:: configured

    **CRUD operations**

    .. automethod:: get
    .. automethod:: get_many
    .. automethod:: put
    .. automethod:: delete
    .. automethod:: delete_all
    .. automethod:: delete_index

    **Keyword scanning**

    .. automethod:: keyword_scan
    .. automethod:: keyword_scan_ids

    **Scanning ids in lexicographic order**

    Note that these operations may be inefficient because of
    how ElasticSearch handles sorting.

    .. automethod:: scan
    .. automethod:: scan_ids
    .. automethod:: scan_prefix
    .. automethod:: scan_prefix_ids

    **Low-level**

    .. automethod:: sync
    .. automethod:: index_scan_ids
    .. automethod:: index_names
    '''
    config_name = 'dossier.store'

    @classmethod
    def configured(cls):
        '''Create a new instance from the global configuration.

        In order to use this, you must make sure that
        :class:`ElasticStore` has been configured by :mod:`yakonfig`,
        usually by passing the class to ``yakonfig.parse_args``.
        '''
        return cls(**yakonfig.get_global_config('dossier.store'))

    def __init__(self, hosts=None, namespace=None, type='fc',
                 feature_indexes=None, shards=10, replicas=0,
                 fulltext_indexes=None):
        '''Create a new store or connect to an existing one.

        :param hosts:
          Passed directly to ``elasticsearch.Elasticsearch``
          constructor. Required.
        :param str namespace:
          Used as the ES index name, prefixed by ``fcs_``. Required.
        :param str type:
          The ES type to use. If this is set to ``None``, then a random
          unique string is used.
        :param [str] feature_indexes:
          A list of names of features to index.
        :param int shards:
          The number of shards to use for this index. This only has an
          effect if the ES index didn't previous exist.
        :param int replicas:
          The number of replicas to use for this index. This only has
          an effect if the ES index didn't previous exist.
        :rtype: :class:`ElasticStore`
        '''
        if hosts is None:
            raise yakonfig.ProgrammerError(
                'ElasticStore needs at least one host specified.')
        if namespace is None:
            raise yakonfig.ProgrammerError(
                'ElasticStore needs a namespace defined.')
        if type is None:
            type = unicode(uuid.uuid4())
        self.conn = Elasticsearch(hosts=hosts, timeout=60, request_timeout=60)
        self.index = 'fcs_%s' % namespace
        self.type = type
        self.shards = shards
        self.replicas = replicas
        self.indexes = OrderedDict()
        self.fulltext_indexes = OrderedDict()
        self.indexed_features = set()
        self.fulltext_indexed_features = set()

        self._normalize_feature_indexes(feature_indexes)
        self._normalize_fulltext_feature_indexes(fulltext_indexes)
        if not self.conn.indices.exists(index=self.index):
            # This can race, but that should be OK.
            # Worst case, we initialize with the same settings more than
            # once.
            self._create_index()
        mapping = self.conn.indices.get_mapping(
            index=self.index, doc_type=self.type)
        if len(mapping) == 0:
            self._create_mappings()

    def get(self, content_id, feature_names=None):
        '''Retrieve a feature collection.

        If a feature collection with the given id does not
        exist, then ``None`` is returned.

        :param str content_id: Content identifier.
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        :rtype: :class:`dossier.fc.FeatureCollection` or ``None``
        '''
        try:
            resp = self.conn.get(index=self.index, doc_type=self.type,
                                 id=eid(content_id),
                                 _source=self._source(feature_names))
            return self.fc_from_dict(resp['_source']['fc'])
        except NotFoundError:
            return None
        except:
            raise

    def get_many(self, content_ids, feature_names=None):
        '''Returns an iterable of feature collections.

        This efficiently retrieves multiple FCs corresponding to the
        list of ids given. Tuples of identifier and feature collection
        are yielded. If the feature collection for a given id does not
        exist, then ``None`` is returned as the second element of the
        tuple.

        :param [str] content_ids: List of content ids.
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        :rtype: Iterable of ``(content_id, FC)``
        '''
        try:
            resp = self.conn.mget(index=self.index, doc_type=self.type,
                                  _source=self._source(feature_names),
                                  body={'ids': map(eid, content_ids)})
        except TransportError:
            return
        for doc in resp['docs']:
            fc = None
            if doc['found']:
                fc = self.fc_from_dict(doc['_source']['fc'])
            yield did(doc['_id']), fc

    def put(self, items, indexes=True):
        '''Adds feature collections to the store.

        This efficiently adds multiple FCs to the store. The iterable
        of ``items`` given should yield tuples of ``(content_id, FC)``.

        :param items: Iterable of ``(content_id, FC)``.
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        '''
        actions = []
        for cid, fc in items:
            # TODO: If we store features in a columnar order, then we
            # could tell ES to index the feature values directly. ---AG
            # (But is problematic because we want to preserve the ability
            # to selectively index FCs. So we'd probably need two distinct
            # doc types.)
            idxs = defaultdict(list)
            if indexes:
                for fname in self.indexed_features:
                    if fname in fc:
                        idxs[fname_to_idx_name(fname)].extend(fc[fname])
                for fname in self.fulltext_indexed_features:
                    if fname not in fc:
                        continue
                    if isinstance(fc[fname], basestring):
                        idxs[fname_to_full_idx_name(fname)] = fc[fname]
                    else:
                        idxs[fname_to_full_idx_name(fname)].extend(fc[fname])
            actions.append({
                '_index': self.index,
                '_type': self.type,
                '_id': eid(cid),
                '_op_type': 'index',
                '_source': dict(idxs, **{
                    'fc': self.fc_to_dict(fc),
                }),
            })
        bulk(self.conn, actions, timeout=60, request_timeout=60)

    def delete(self, content_id):
        '''Deletes the corresponding feature collection.

        If the FC does not exist, then this is a no-op.
        '''
        try:
            self.conn.delete(index=self.index, doc_type=self.type,
                             id=eid(content_id))
        except NotFoundError:
            pass

    def delete_all(self):
        '''Deletes all feature collections.

        This does not destroy the ES index, but instead only
        deletes all FCs with the configured document type
        (defaults to ``fc``).
        '''
        try:
            self.conn.indices.delete_mapping(
                index=self.index, doc_type=self.type)
        except TransportError:
            logger.warn('type %r in index %r already deleted',
                        self.index, self.type, exc_info=True)

    def delete_index(self):
        '''Deletes the underlying ES index.

        Only use this if you know what you're doing. This destroys
        the entire underlying ES index, which could be shared by
        multiple distinct ElasticStore instances.
        '''
        if self.conn.indices.exists(index=self.index):
            self.conn.indices.delete(index=self.index)

    def sync(self):
        '''Tells ES to tell Lucene to do an fsync.

        This guarantees that any previous calls to ``put`` will be
        flushed to disk and available in subsequent searches.

        Generally, this should only be used in test code.
        '''
        self.conn.indices.refresh(index=self.index)

    def scan(self, *key_ranges, **kwargs):
        '''Scan for FCs in the given id ranges.

        :param key_ranges:
          ``key_ranges`` should be a list of pairs of ranges. The first
          value is the lower bound id and the second value is the
          upper bound id. Use ``()`` in either position to leave it
          unbounded. If no ``key_ranges`` are given, then all FCs in
          the store are returned.
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        :rtype: Iterable of ``(content_id, FC)``
        '''
        for hit in self._scan(*key_ranges, **kwargs):
            yield did(hit['_id']), self.fc_from_dict(hit['_source']['fc'])

    def scan_ids(self, *key_ranges, **kwargs):
        '''Scan for ids only in the given id ranges.

        :param key_ranges:
          ``key_ranges`` should be a list of pairs of ranges. The first
          value is the lower bound id and the second value is the
          upper bound id. Use ``()`` in either position to leave it
          unbounded. If no ``key_ranges`` are given, then all FCs in
          the store are returned.
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        :rtype: Iterable of ``content_id``
        '''
        kwargs['feature_names'] = False
        for hit in self._scan(*key_ranges, **kwargs):
            yield did(hit['_id'])

    def scan_prefix(self, prefix, feature_names=None):
        '''Scan for FCs with a given prefix.

        :param str prefix: Identifier prefix.
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        :rtype: Iterable of ``(content_id, FC)``
        '''
        resp = self._scan_prefix(prefix, feature_names=feature_names)
        for hit in resp:
            yield did(hit['_id']), self.fc_from_dict(hit['_source']['fc'])

    def scan_prefix_ids(self, prefix):
        '''Scan for ids with a given prefix.

        :param str prefix: Identifier prefix.
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        :rtype: Iterable of ``content_id``
        '''
        resp = self._scan_prefix(prefix, feature_names=False)
        for hit in resp:
            yield did(hit['_id'])

    def fulltext_scan(self, query_id=None, query_fc=None, feature_names=None,
                      preserve_order=True):
        '''Fulltext search.

        Yields an iterable of triples (score, identifier, FC)
        corresponding to the search results of the fulltext search
        in ``query``. This will only search text indexed under the
        given feature named ``fname``.

        Note that, unless ``preserve_order`` is set to True, the
        ``score`` will always be 0.0, and the results will be
        unordered. ``preserve_order`` set to True will cause the
        results to be scored and be ordered by score, but you should
        expect to see a decrease in performance.

        :param str fname:
          The feature to search.
        :param unicode query:
          The query.
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        :rtype: Iterable of ``(score, content_id, FC)``
        '''
        it = self._fulltext_scan(query_id, query_fc,
                                 feature_names=feature_names,
                                 preserve_order=preserve_order)
        for hit in it:
            fc = self.fc_from_dict(hit['_source']['fc'])
            yield hit['_score'], did(hit['_id']), fc

    def fulltext_scan_ids(self, query_id=None, query_fc=None,
                          preserve_order=True):
        '''Fulltext search for identifiers.

        Yields an iterable of triples (score, identifier)
        corresponding to the search results of the fulltext search
        in ``query``. This will only search text indexed under the
        given feature named ``fname``.

        Note that, unless ``preserve_order`` is set to True, the
        ``score`` will always be 0.0, and the results will be
        unordered. ``preserve_order`` set to True will cause the
        results to be scored and be ordered by score, but you should
        expect to see a decrease in performance.

        :param str fname:
          The feature to search.
        :param unicode query:
          The query.
        :rtype: Iterable of ``(score, content_id)``
        '''
        it = self._fulltext_scan(query_id, query_fc, feature_names=False,
                                 preserve_order=preserve_order)
        for hit in it:
            yield hit['_score'], did(hit['_id'])

    def keyword_scan(self, query_id=None, query_fc=None, feature_names=None):
        '''Keyword scan for feature collections.

        This performs a keyword scan using the query given. A keyword
        scan searches for FCs with terms in each of the query's indexed
        fields.

        At least one of ``query_id`` or ``query_fc`` must be provided.
        If ``query_fc`` is ``None``, then the query is retrieved
        automatically corresponding to ``query_id``.

        :param str query_id: Optional query id.
        :param query_fc: Optional query feature collection.
        :type query_fc: :class:`dossier.fc.FeatureCollection`
        :param [str] feature_names:
          A list of feature names to retrieve. When ``None``, all
          features are retrieved. Wildcards are allowed.
        :rtype: Iterable of ``(content_id, FC)``
        '''
        it = self._keyword_scan(query_id, query_fc,
                                feature_names=feature_names)
        for hit in it:
            fc = self.fc_from_dict(hit['_source']['fc'])
            yield did(hit['_id']), fc

    def keyword_scan_ids(self, query_id=None, query_fc=None):
        '''Keyword scan for ids.

        This performs a keyword scan using the query given. A keyword
        scan searches for FCs with terms in each of the query's indexed
        fields.

        At least one of ``query_id`` or ``query_fc`` must be provided.
        If ``query_fc`` is ``None``, then the query is retrieved
        automatically corresponding to ``query_id``.

        :param str query_id: Optional query id.
        :param query_fc: Optional query feature collection.
        :type query_fc: :class:`dossier.fc.FeatureCollection`
        :rtype: Iterable of ``content_id``
        '''
        it = self._keyword_scan(query_id, query_fc, feature_names=False)
        for hit in it:
            yield did(hit['_id'])

    def index_scan_ids(self, fname, val):
        '''Low-level keyword index scan for ids.

        Retrieves identifiers of FCs that have a feature value
        ``val`` in the feature named ``fname``. Note that
        ``fname`` must be indexed.

        :param str fname: Feature name.
        :param str val: Feature value.
        :rtype: Iterable of ``content_id``
        '''
        disj = []
        for fname2 in self.indexes[fname]['feature_names']:
            disj.append({'term': {fname_to_idx_name(fname2): val}})
        query = {
            'constant_score': {
                'filter': {'or': disj},
            },
        }
        hits = scan(self.conn, index=self.index, doc_type=self.type, query={
            '_source': False,
            'query': query,
        })
        for hit in hits:
            yield did(hit['_id'])

    def index_names(self):
        '''Returns a list of all defined index names.

        Note that this only includes boolean based indexes.

        :rtype: list of ``unicode``
        '''
        return map(unicode, self.indexes.iterkeys())

    def fulltext_index_names(self):
        '''Returns a list of all defined fulltext index names.

        :rtype: list of ``unicode``
        '''
        return map(unicode, self.fulltext_indexes.iterkeys())

    def _fulltext_scan(self, query_id, query_fc, preserve_order=True,
                       feature_names=None):
        query_fc = self.get_query_fc(query_id, query_fc)
        ids = set([] if query_id is None else [eid(query_id)])
        for fname, features in self.fulltext_indexes.iteritems():
            qvals = map(unicode, query_fc.get(fname, {}).keys())
            if len(qvals) == 0:
                continue
            query = {
                'filtered': {
                    'query': {
                        'multi_match': {
                            'type': 'phrase',
                            'query': ' '.join(qvals),
                            'fields': map(fname_to_full_idx_name, features),
                        },
                    },
                    'filter': {
                        'not': {
                            'ids': {
                                'values': list(ids),
                            },
                        },
                    },
                },
            }

            logger.info('fulltext scanning index: %s, query: %r', fname, qvals)
            hits = scan(
                self.conn, index=self.index, doc_type=self.type,
                preserve_order=preserve_order,
                query={
                    '_source': self._source(feature_names),
                    'query': query,
                })
            for hit in hits:
                ids.add(eid(hit['_id']))
                yield hit

    def _keyword_scan(self, query_id, query_fc, feature_names=None):
        # Why are we running multiple scans? Why are we deduplicating?
        #
        # It turns out that, in our various systems, it can be important to
        # prioritize the order of results returned in a keyword scan based on
        # the feature index that is being searched. For example, we typically
        # want to start a keyword scan with the results from a search on
        # `NAME`, which we don't want to be mingled with the results from a
        # search on some other feature.
        #
        # The simplest way to guarantee this type of prioritization is to run
        # a query for each index in the order in which they were defined.
        #
        # This has some downsides:
        #
        # 1. We return *all* results for the first index before ever returning
        #    results for the second.
        # 2. Since we're running multiple queries, we could get back results
        #    we've already retrieved in a previous query.
        #
        # We accept (1) for now.
        #
        # To fix (2), we keep track of all ids we've seen and include them
        # as a filter in subsequent queries.
        query_fc = self.get_query_fc(query_id, query_fc)
        ids = set([] if query_id is None else [eid(query_id)])
        for fname in self.indexes:
            term_disj = self._fc_index_disjunction_from_query(query_fc, fname)
            if len(term_disj) == 0:
                continue
            query = {
                'constant_score': {
                    'filter': {
                        'and': [{
                            'not': {
                                'ids': {
                                    'values': list(ids),
                                },
                            },
                        }, {
                            'or': term_disj,
                        }],
                    },
                },
            }

            logger.info('keyword scanning index: %s', fname)
            hits = scan(
                self.conn, index=self.index, doc_type=self.type,
                query={
                    '_source': self._source(feature_names),
                    'query': query,
                })
            for hit in hits:
                ids.add(eid(hit['_id']))
                yield hit

    def _scan(self, *key_ranges, **kwargs):
        feature_names = kwargs.get('feature_names')
        range_filters = self._range_filters(*key_ranges)
        return scan(self.conn, index=self.index, doc_type=self.type,
                    _source=self._source(feature_names),
                    preserve_order=True,
                    query={
                        # Sorting by `_id` seems to fail spuriously and
                        # I have no idea why. ---AG
                        'sort': {'_uid': {'order': 'asc'}},
                        'query': {
                            'constant_score': {
                                'filter': {
                                    'and': range_filters,
                                },
                            },
                        },
                    })

    def _scan_prefix(self, prefix, feature_names=None):
        query = {
            'constant_score': {
                'filter': {
                    'and': [{
                        'prefix': {
                            '_id': eid(prefix),
                        },
                    }],
                },
            },
        }
        return scan(self.conn, index=self.index, doc_type=self.type,
                    _source=self._source(feature_names),
                    preserve_order=True,
                    query={
                        # Sorting by `_id` seems to fail spuriously and
                        # I have no idea why. ---AG
                        'sort': {'_uid': {'order': 'asc'}},
                        'query': query,
                    })

    def _source(self, feature_names):
        '''Maps feature names to ES's "_source" field.'''
        if feature_names is None:
            return True
        elif isinstance(feature_names, bool):
            return feature_names
        else:
            return map(lambda n: 'fc.' + n, feature_names)

    def _range_filters(self, *key_ranges):
        'Creates ES filters for key ranges used in scanning.'
        filters = []
        for s, e in key_ranges:
            if isinstance(s, basestring):
                s = eid(s)
            if isinstance(e, basestring):
                # Make the range inclusive.
                # We need a valid codepoint, so use the max.
                e += u'\U0010FFFF'
                e = eid(e)

            if s == () and e == ():
                filters.append({'match_all': {}})
            elif e == ():
                filters.append({'range': {'_id': {'gte': s}}})
            elif s == ():
                filters.append({'range': {'_id': {'lte': e}}})
            else:
                filters.append({'range': {'_id': {'gte': s, 'lte': e}}})
        if len(filters) == 0:
            return [{'match_all': {}}]
        else:
            return filters

    def _create_index(self):
        'Create the index'
        try:
            self.conn.indices.create(
                index=self.index, timeout=60, request_timeout=60, body={
                    'settings': {
                        'number_of_shards': self.shards,
                        'number_of_replicas': self.replicas,
                    },
                })
        except TransportError:
            # Hope that this is an "index already exists" error...
            logger.warn('index already exists? OK', exc_info=True)
            pass

    def _create_mappings(self):
        'Create the field type mapping.'
        self.conn.indices.put_mapping(
            index=self.index, doc_type=self.type,
            timeout=60, request_timeout=60,
            body={
                self.type: {
                    'dynamic_templates': [{
                        'default_no_analyze_fc': {
                            'match': 'fc.*',
                            'mapping': {'index': 'no'},
                        },
                    }],
                    '_all': {
                        'enabled': False,
                    },
                    '_id': {
                        'index': 'not_analyzed',  # allows range queries
                    },
                    'properties': self._get_index_mappings(),
                },
            })
        # It is possible to create an index and quickly launch a request
        # that will fail because the index hasn't been set up yet. Usually,
        # you'll get a "no active shards available" error.
        #
        # Since index creation is a very rare operation (it only happens
        # when the index doesn't already exist), we sit and wait for the
        # cluster to become healthy.
        self.conn.cluster.health(index=self.index, wait_for_status='yellow')

    def _get_index_mappings(self):
        'Retrieve the field mappings. Useful for debugging.'
        maps = {}
        for fname in self.indexed_features:
            config = self.indexes.get(fname, {})
            print(fname, config)
            maps[fname_to_idx_name(fname)] = {
                'type': config.get('es_index_type', 'integer'),
                'store': False,
                'index': 'not_analyzed',
            }
        for fname in self.fulltext_indexed_features:
            maps[fname_to_full_idx_name(fname)] = {
                'type': 'string',
                'store': False,
                'index': 'analyzed',
            }
        return maps

    def _get_field_types(self):
        'Retrieve the field types. Useful for debugging.'
        mapping = self.conn.indices.get_mapping(
            index=self.index, doc_type=self.type)
        return mapping[self.index]['mappings'][self.type]['properties']

    def _normalize_fulltext_feature_indexes(self, fulltext_indexes):
        for x in fulltext_indexes or []:
            if isinstance(x, Mapping):
                assert len(x) == 1, 'only one mapping per index entry allowed'
                name = x.keys()[0]
                features = x[name]
            else:
                name = x
                features = [x]
            self.fulltext_indexes[name] = features
            for fname in features:
                self.fulltext_indexed_features.add(fname)

    def _normalize_feature_indexes(self, feature_indexes):
        for x in feature_indexes or []:
            if isinstance(x, Mapping):
                assert len(x) == 1, 'only one mapping per index entry allowed'
                name = x.keys()[0]
                if isinstance(x[name], Mapping):
                    index_type = x[name]['es_index_type']
                    features = x[name]['feature_names']
                else:
                    index_type = 'integer'
                    features = x[name]
            else:
                name = x
                features = [x]
                index_type = 'integer'
            self.indexes[name] = {
                'feature_names': features,
                'es_index_type': index_type,
            }
            for fname in features:
                self.indexed_features.add(fname)

    def _fc_index_disjunction_from_query(self, query_fc, fname):
        'Creates a disjunction for keyword scan queries.'
        if len(query_fc.get(fname, [])) == 0:
            return []
        terms = query_fc[fname].keys()

        disj = []
        for fname in self.indexes[fname]['feature_names']:
            disj.append({'terms': {fname_to_idx_name(fname): terms}})
        return disj

    def fc_to_dict(self, fc):
        d = {}
        for name, feat in fc.to_dict().iteritems():
            # This is a hack to drop the clean_visible feature because it
            # is not necessary to store it and it is large. We simply need
            # to index it.
            if name == '#clean_visible':
                continue
            d[name] = base64.b64encode(cbor.dumps(feat))
        return d

    def fc_from_dict(self, fc_dict):
        d = {}
        for name, feat in fc_dict.iteritems():
            d[name] = cbor.loads(base64.b64decode(feat))
        return FC(d)

    def get_query_fc(self, query_id, query_fc):
        if query_fc is None:
            if query_id is None:
                raise ValueError(
                    'one of query_id or query_fc must not be None')
            query_fc = self.get(query_id)
        if query_fc is None:
            raise KeyError(query_id)
        return query_fc


class ElasticStoreSync(ElasticStore):
    '''Synchronous ElasticSearch backend.

    This is just like :class:`ElasticStore`, except it will call `sync`
    after every ``put`` and ``delete`` operation.

    This is useful for testing where it is most convenient for every
    write operation to be synchronous.
    '''
    def put(self, *args, **kwargs):
        super(ElasticStoreSync, self).put(*args, **kwargs)
        self.sync()

    def delete(self, *args, **kwargs):
        super(ElasticStoreSync, self).delete(*args, **kwargs)
        self.sync()


def eid(s):
    '''Encode id (bytes) as a Unicode string.

    The encoding is done such that lexicographic order is
    preserved. No concern is given to wasting space.

    The inverse of ``eid`` is ``did``.
    '''
    if isinstance(s, unicode):
        s = s.encode('utf-8')
    return u''.join('{:02x}'.format(ord(b)) for b in s)


def did(s):
    '''Decode id (Unicode string) as a bytes.

    The inverse of ``did`` is ``eid``.
    '''
    return ''.join(chr(int(s[i:i+2], base=16)) for i in xrange(0, len(s), 2))


def idx_name_to_fname(idx_name):
    return idx_name[4:]


def fname_to_idx_name(fname):
    return u'idx_%s' % fname.decode('utf-8')


def full_idx_name_to_fname(idx_name):
    return idx_name[9:]


def fname_to_full_idx_name(fname):
    return u'full_idx_%s' % fname.decode('utf-8')
