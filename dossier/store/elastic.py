'''A native ElasticSearch implementation for dossier.store.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import base64
from collections import OrderedDict, Mapping, defaultdict
import logging

import cbor
from dossier.fc import FeatureCollection as FC

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk, scan

logger = logging.getLogger(__name__)


class ElasticStore(object):
    def __init__(self, hosts=None, namespace=None, feature_indexes=None,
                 shards=10, replicas=0):
        self.conn = Elasticsearch(hosts=hosts)
        self.index = '%s_fcs' % namespace
        self.type = 'fc'
        self.shards = shards
        self.replicas = replicas
        self._normalize_feature_indexes(feature_indexes)

        if not self.conn.indices.exists(index=self.index):
            # This can race, but that should be OK.
            # Worst case, we initialize with the same settings more than
            # once.
            self._create()

    def get(self, content_id, feature_names=None):
        try:
            resp = self.conn.get(index=self.index, doc_type=self.type,
                                 id=eid(content_id),
                                 _source=self._source(feature_names))
            return fc_from_dict(resp['_source']['fc'])
        except NotFoundError:
            return None
        except:
            raise

    def get_many(self, content_id_list, feature_names=None):
        resp = self.conn.mget(index=self.index, doc_type=self.type,
                              _source=self._source(feature_names),
                              body={'ids': map(eid, content_id_list)})
        for doc in resp['docs']:
            fc = fc_from_dict(doc['_source']['fc']) if doc['found'] else None
            yield did(doc['_id']), fc

    def put(self, items, fc_type='fc'):
        actions = []
        for cid, fc in items:
            idxs = defaultdict(list)
            for idx_name, config in self.indexes.iteritems():
                for fname in config['feature_names']:
                    idxs[idx_name].extend(fc[fname])
            actions.append({
                '_index': self.index,
                '_type': self.type,
                '_id': eid(cid),
                '_op_type': 'index',
                '_source': dict(idxs, **{
                    'fc_type': fc_type,
                    'fc': fc_to_dict(fc),
                }),
            })
        bulk(self.conn, actions, timeout=60, request_timeout=60)

    def sync(self):
        self.conn.indices.refresh(index=self.index)

    def scan(self, *key_ranges, **kwargs):
        for hit in self._scan(*key_ranges, **kwargs)['hits']['hits']:
            yield fc_from_dict(hit['_source']['fc'])

    def scan_ids(self, *key_ranges, **kwargs):
        kwargs['feature_names'] = False
        resp = self._scan(*key_ranges, **kwargs)
        for hit in resp['hits']['hits']:
            yield did(hit['_id'])

    def scan_prefix(self, prefix, feature_names=None, fc_type='fc'):
        resp = self._scan_prefix(prefix, feature_names=feature_names,
                                 fc_type=fc_type)
        for hit in resp['hits']['hits']:
            yield fc_from_dict(hit['_source']['fc'])

    def scan_prefix_ids(self, prefix, fc_type='fc'):
        resp = self._scan_prefix(prefix, feature_names=False, fc_type=fc_type)
        for hit in resp['hits']['hits']:
            yield did(hit['_id'])

    def delete(self, content_id):
        self.conn.delete(index=self.index, doc_type=self.type,
                         id=eid(content_id))

    def delete_all(self):
        if self.conn.indices.exists(index=self.index):
            self.conn.indices.delete(index=self.index)

    def canopy_scan(self, query_id, query_fc=None,
                    feature_names=None, fc_type=None):
        it = self._canopy_scan(query_id, query_fc,
                               feature_names=feature_names, fc_type=fc_type)
        for hit in it:
            yield did(hit['_id']), fc_from_dict(hit['_source']['fc'])

    def canopy_scan_ids(self, query_id, query_fc=None, fc_type=None):
        it = self._canopy_scan(query_id, query_fc, feature_names=False,
                               fc_type=fc_type)
        for hit in it:
            yield did(hit['_id'])

    def index_scan(self, idx_name, val, fc_type=None):
        query = {
            'constant_score': {
                'filter': {
                    'and': [{
                        'term': {
                            'idx_' + idx_name: val,
                        },
                    }],
                },
            },
        }
        self._add_fc_type_to_and(
            query['constant_score']['filter']['and'], fc_type)
        hits = scan(self.conn, query={
            '_source': False,
            'query': query,
        })
        for hit in hits:
            yield did(hit['_id'])

    def _canopy_scan(self, query_id, query_fc,
                     feature_names=None, fc_type=None):
        # Why are we running multiple scans? Why are we deduplicating?
        #
        # It turns out that, in our various systems, it can be important to
        # prioritize the order of results returned in a canopy scan based on
        # the feature index that is being searched. For example, we typically
        # want to start a canopy scan with the results from a search on `NAME`,
        # which we don't want to be mingled with the results from a search on
        # some other feature.
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
        if query_fc is None:
            # I think we can actually tell ES to pull the fields directly
            # from the query server-side, but that's a premature optimization
            # at this point. ---AG
            query_fc = self.get(query_id)
        if query_fc is None:
            raise KeyError(query_id)
        ids = set([eid(query_id)])
        for iname in self.indexes:
            fname = iname[4:]
            terms = query_fc[fname].keys()
            if len(terms) == 0:
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
                            'terms': {
                                iname: terms,
                            },
                        }],
                    },
                },
            }
            self._add_fc_type_to_and(
                query['constant_score']['filter']['and'], fc_type)

            logger.info('canopy scanning index: %s', fname)
            hits = scan(self.conn, query={
                '_source': self._source(feature_names),
                'query': query,
            })
            for hit in hits:
                ids.add(eid(hit['_id']))
                yield hit

    def _scan(self, *key_ranges, **kwargs):
        feature_names = kwargs.get('feature_names')
        range_filters = self._range_filters(*key_ranges)
        self._add_fc_type_to_and(range_filters, kwargs.get('fc_type'))
        return self.conn.search(index=self.index, doc_type=self.type,
                                _source=self._source(feature_names),
                                body={
                                    'sort': {'_id': {'order': 'asc'}},
                                    'query': {
                                        'constant_score': {
                                            'filter': {
                                                'and': range_filters,
                                            },
                                        },
                                    },
                                })

    def _scan_prefix(self, prefix, feature_names=None, fc_type=None):
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
        self._add_fc_type_to_and(
            query['constant_score']['filter']['and'], fc_type)
        return self.conn.search(index=self.index, doc_type=self.type,
                                _source=self._source(feature_names),
                                body={
                                    'sort': {'_id': {'order': 'asc'}},
                                    'query': query,
                                })

    def _source(self, feature_names):
        if feature_names is None:
            return True
        elif isinstance(feature_names, bool):
            return feature_names
        else:
            return map(lambda n: 'fc.' + n, feature_names)

    def _range_filters(self, *key_ranges):
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

    def _create(self):
        self.conn.indices.create(index=self.index, body={
            'settings': {
                'number_of_shards': self.shards,
                'number_of_replicas': self.replicas,
            },
        })
        self.conn.indices.put_mapping(
            index=self.index, doc_type=self.type, body={
                'fc': {
                    'dynamic_templates': [{
                        'default_no_analyze_fc': {
                            'match': 'fc.*',
                            'mapping': {'index': 'no'},
                        },
                    }],
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
        maps = {}
        for fname, config in self.indexes.iteritems():
            maps[fname] = {
                'type': config['es_index_type'],
                'store': False,
                'index': 'not_analyzed',
            }
        return maps

    def _normalize_feature_indexes(self, feature_indexes):
        self.indexes = OrderedDict()
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
            name = 'idx_%s' % name
            self.indexes[name.decode('utf-8')] = {
                'feature_names': features,
                'es_index_type': index_type,
            }

    def _add_fc_type_to_and(self, and_filter, fc_type):
        if fc_type is not None:
            and_filter.append({'term': {'fc_type': fc_type}})


def fc_to_dict(fc):
    d = {}
    for name, feat in fc.to_dict().iteritems():
        d[name] = base64.b64encode(cbor.dumps(feat))
    return d


def fc_from_dict(fc_dict):
    d = {}
    for name, feat in fc_dict.iteritems():
        d[name] = cbor.loads(base64.b64decode(feat))
    return FC(d)


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
