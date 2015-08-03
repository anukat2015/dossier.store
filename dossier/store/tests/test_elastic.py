'''Test the ElasticSearch backend.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
import logging

import pytest

from dossier.fc import FeatureCollection as FC
from dossier.store.elastic import ElasticStore

from dossier.store.tests import kvl  # noqa


logger = logging.getLogger(__name__)


@pytest.yield_fixture  # noqa
def store():
    s = ElasticStore(feature_indexes=[{
        'NAME': {'es_index_type': 'string', 'feature_names': ['NAME']},
    }, {
        'boNAME': {'es_index_type': 'string', 'feature_names': ['boNAME']},
    }])
    yield s
    s.delete_all()


def test_basic(store):
    print(store.conn.indices.get_settings(store.index))
    print(store.conn.indices.get_mapping(store.index))
    fc = FC({
        'NAME': {
            'bruce': 2,
            'springsteen': 1,
        },
        'boNAME': {
            'foo': 1,
            'bar': 2,
        },
    })
    store.put([('a', fc)])
    store.conn.indices.refresh(index=store.index)
    results = store.conn.search(
        index=store.index, doc_type=store.type,
        _source=['fc.NAME'],
        body={
            'query': {
                'constant_score': {
                    'filter': {
                        'terms': {
                            'idx_NAME': fc['NAME'].keys(),
                        },
                    },
                },
            },
        })
    import pprint
    pprint.pprint(results)
    print('-' * 79)
    pprint.pprint(list(store.canopy_scan(fc)))
    assert False
