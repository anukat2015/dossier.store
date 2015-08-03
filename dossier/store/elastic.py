'''A native ElasticSearch implementation for dossier.store.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

from collections import OrderedDict, Mapping, defaultdict
import logging

from dossier.fc import FeatureCollection as FC

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan

logger = logging.getLogger(__name__)


class ElasticStore(object):
    def __init__(self, hosts=None, namespace='d01', feature_indexes=None):
        self.conn = Elasticsearch(hosts=hosts)
        self.index = '%s_fcs' % namespace
        self.type = 'fc'
        self._normalize_feature_indexes(feature_indexes)

        if not self.conn.indices.exists(index=self.index):
            # This can race, but that should be OK.
            # Worst case, we initialize with the same settings more than
            # once.
            self._create()

    def get(self, content_id, feature_names=None):
        resp = self.conn.get(index=self.index, doc_type=self.type,
                             id=content_id,
                             _source=self._source(feature_names))
        return FC(resp['_source']['fc'])

    def get_many(self, content_id_list, feature_names=None):
        resp = self.conn.mget(index=self.index, doc_type=self.type,
                              _source=self._source(feature_names),
                              body={'ids': content_id_list})
        for doc in resp['docs']:
            yield FC(doc['_source']['fc'])

    def put(self, items, indexes=True):
        actions = []
        for cid, fc in items:
            idxs = defaultdict(list)
            for idx_name, config in self.indexes.iteritems():
                for fname in config['feature_names']:
                    idxs[idx_name].extend(fc[fname])
            actions.append({
                '_index': self.index,
                '_type': self.type,
                '_id': cid,
                '_op_type': 'index',
                '_source': dict(idxs, **{
                    'fc': fc.to_dict(),
                }),
            })
        bulk(self.conn, actions)

    def scan(self, *key_ranges, **kwargs):
        for hit in self._scan(*key_ranges, **kwargs)['hits']['hits']:
            yield FC(hit['_source']['fc'])

    def scan_ids(self, *key_ranges):
        resp = self._scan(*key_ranges, feature_names=False)
        for hit in resp['hits']['hits']:
            yield hit['_id']

    def scan_prefix(self, prefix, feature_names=None):
        resp = self._scan_prefix(prefix, feature_names=feature_names)
        for hit in resp['hits']['hits']:
            yield FC(hit['_source']['fc'])

    def scan_prefix_ids(self, prefix):
        resp = self._scan_prefix(prefix, feature_names=False)
        for hit in resp['hits']['hits']:
            yield hit['_id']

    def delete(self, content_id):
        self.conn.delete(index=self.index, doc_type=self.type, id=content_id)

    def delete_all(self):
        if self.conn.indices.exists(index=self.index):
            self.conn.indices.delete(index=self.index)

    def canopy_scan(self, query_fc, feature_names=None):
        for hit in self._canopy_scan(query_fc, feature_names=feature_names):
            yield FC(hit['_source']['fc'])

    def canopy_scan_ids(self, query_fc):
        for hit in self._canopy_scan(query_fc, feature_names=False):
            yield hit['_id']

    def _canopy_scan(self, query_fc, feature_names=None):
        ids = set()
        for iname in self.indexes:
            fname = iname[4:]
            terms = query_fc[fname].keys()
            hits = scan(self.conn, query={
                '_source': self._source(feature_names),
                'query': {
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
                },
            })
            for hit in hits:
                ids.add(hit['_id'])
                yield hit

    def _scan(self, *key_ranges, **kwargs):
        feature_names = kwargs.get('feature_names')
        range_filters = self._range_filters(*key_ranges)
        return self.conn.search(index=self.index, doc_type=self.type,
                                _source=self._source(feature_names),
                                body={
                                    'query': {
                                        'constant_score': {
                                            'filter': {
                                                'and': range_filters,
                                            },
                                        },
                                    },
                                })

    def _scan_prefix(self, prefix, feature_names=None):
        return self.conn.search(index=self.index, doc_type=self.type,
                                _source=self._source(feature_names),
                                body={
                                    'query': {
                                        'constant_score': {
                                            'filter': {
                                                'prefix': {
                                                    '_id': prefix,
                                                },
                                            },
                                        },
                                    },
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
        self.conn.indices.create(index=self.index)
        self.conn.indices.put_mapping(
            index=self.index, doc_type=self.type, body={
                'dynamic_templates': [{
                    'default_no_analyze': {
                        'match': '*',
                        'mapping': {'index': 'no'},
                    },
                }],
                'fc': {
                    'properties': self._get_index_mappings(),
                },
            })

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
