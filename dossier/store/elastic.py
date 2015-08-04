'''A native ElasticSearch implementation for dossier.store.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

from collections import OrderedDict, Mapping, defaultdict
import logging

from dossier.fc import FeatureCollection as FC

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk, scan

logger = logging.getLogger(__name__)


class ElasticStore(object):
    def __init__(self, hosts=None, namespace=None, feature_indexes=None):
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
        try:
            resp = self.conn.get(index=self.index, doc_type=self.type,
                                 id=content_id,
                                 _source=self._source(feature_names))
            return FC(resp['_source']['fc'])
        except NotFoundError:
            return None
        except:
            raise

    def get_many(self, content_id_list, feature_names=None):
        resp = self.conn.mget(index=self.index, doc_type=self.type,
                              _source=self._source(feature_names),
                              body={'ids': content_id_list})
        for doc in resp['docs']:
            fc = FC(doc['_source']['fc']) if doc['found'] else None
            yield doc['_id'], fc

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
                '_id': cid,
                '_op_type': 'index',
                '_source': dict(idxs, **{
                    'fc_type': fc_type,
                    'fc': fc.to_dict(),
                }),
            })
        bulk(self.conn, actions)

    def sync(self):
        self.conn.indices.refresh(index=self.index)

    def scan(self, *key_ranges, **kwargs):
        for hit in self._scan(*key_ranges, **kwargs)['hits']['hits']:
            yield FC(hit['_source']['fc'])

    def scan_ids(self, *key_ranges, **kwargs):
        kwargs['feature_names'] = False
        resp = self._scan(*key_ranges, **kwargs)
        for hit in resp['hits']['hits']:
            yield hit['_id']

    def scan_prefix(self, prefix, feature_names=None, fc_type='fc'):
        resp = self._scan_prefix(prefix, feature_names=feature_names,
                                 fc_type=fc_type)
        for hit in resp['hits']['hits']:
            yield FC(hit['_source']['fc'])

    def scan_prefix_ids(self, prefix, fc_type='fc'):
        resp = self._scan_prefix(prefix, feature_names=False, fc_type=fc_type)
        for hit in resp['hits']['hits']:
            yield hit['_id']

    def delete(self, content_id):
        self.conn.delete(index=self.index, doc_type=self.type, id=content_id)

    def delete_all(self):
        if self.conn.indices.exists(index=self.index):
            self.conn.indices.delete(index=self.index)

    def canopy_scan(self, query_id, query_fc=None,
                    feature_names=None, fc_type=None):
        it = self._canopy_scan(query_id, query_fc,
                               feature_names=feature_names, fc_type=fc_type)
        for hit in it:
            yield hit['_id'], FC(hit['_source']['fc'])

    def canopy_scan_ids(self, query_id, query_fc=None, fc_type=None):
        it = self._canopy_scan(query_id, query_fc, feature_names=False,
                               fc_type=fc_type)
        for hit in it:
            yield hit['_id']

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
            yield hit['_id']

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
        ids = set([query_id])
        for iname in self.indexes:
            fname = iname[4:]
            terms = query_fc[fname].keys()
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
            hits = scan(self.conn, query={
                '_source': self._source(feature_names),
                'query': query,
            })
            for hit in hits:
                ids.add(hit['_id'])
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
                            '_id': prefix,
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
            # Make the range inclusive.
            if isinstance(e, basestring):
                # We need a valid codepoint, so use the max.
                e += u'\U0010FFFF'
            elif isinstance(e, int):
                e += 1

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
