'''Test the ElasticSearch backend.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
import logging
from operator import itemgetter

import pytest

from dossier.fc import FeatureCollection as FC
from dossier.store.elastic import ElasticStoreSync


logger = logging.getLogger(__name__)

NAMESPACE = 'dossier_store_tests'


@pytest.yield_fixture  # noqa
def store(elastic_address, namespace_string):
    s = create_test_store(elastic_address, namespace_string)
    yield s
    s.delete_all()


@pytest.fixture
def fcs():
    return [('boss', FC({
        'NAME': {
            'Bruce Springsteen': 2,
            'The Boss': 1,
        },
        'boNAME': {
            'bruce': 2,
            'springsteen': 5,
            'the': 1,
            'boss': 1,
        },
    })), ('patti', FC({
        'NAME': {
            'Patti Scialfa': 1,
        },
        'boNAME': {
            'patti': 10,
            'scialfa': 1,
        },
    })), ('big-man', FC({
        'NAME': {
            'Clarence Clemons': 8,
            'The Big Man': 1,
        },
        'boNAME': {
            'clarence': 8,
            'clemons': 8,
            'the': 1,
            'big': 1,
            'man': 1,
        },
    }))]


@pytest.fixture
def fcs_texts(fcs):
    return [('boss', FC({
        'NAME': {
            'Bruce Springsteen': 2,
            'The Boss': 1,
        },
        'boNAME': {
            'bruce': 2,
            'springsteen': 5,
            'the': 1,
            'boss': 1,
        },
        'body': {u"The screen door slams, Mary's dress sways": 1},
        'body2': {u"Like a vision she dances across the porch": 1},
    })), ('patti', FC({
        'NAME': {
            'Patti Scialfa': 1,
        },
        'boNAME': {
            'patti': 10,
            'scialfa': 1,
        },
        'body': {u"I come from down in the valley": 1},
    })), ('big-man', FC({
        'NAME': {
            'Clarence Clemons': 8,
            'The Big Man': 1,
        },
        'boNAME': {
            'clarence': 8,
            'clemons': 8,
            'the': 1,
            'big': 1,
            'man': 1,
        },
        'body': {u"Drinking warm beer in the soft summer rain": 1},
    }))]


def create_test_store(host, namespace):
    return ElasticStoreSync(
        hosts=host, namespace=NAMESPACE, type=namespace,
        fulltext_indexes=['body'],
        feature_indexes=[{
            'NAME': {'es_index_type': 'string', 'feature_names': ['NAME']},
        }, {
            'boNAME': {'es_index_type': 'string', 'feature_names': ['boNAME']},
        }])


def fcget(fcs, name1):
    for name2, fc in fcs:
        if name1 == name2:
            return fc
    raise KeyError(name1)


def assert_set_eq(xs, ys):
    # Check equality of two sets of items without caring about order.
    # All that is required is membership testing.
    xs, ys = list(xs), list(ys)
    for x in xs:
        assert x in ys
    for y in ys:
        assert y in xs


def test_put_get(store, fcs):
    fcboss = fcget(fcs, 'boss')
    store.put([('boss', fcboss)])
    assert fcboss == store.get('boss')


def test_get_partial(store, fcs):
    store.put(fcs)
    fc = store.get('boss', feature_names=['NAME'])
    assert 'boNAME' in fcget(fcs, 'boss')
    assert 'boNAME' not in fc


def test_get_many(store, fcs):
    store.put(fcs)
    assert_set_eq(store.get_many(['boss', 'patti']), [
        ('boss', fcget(fcs, 'boss')),
        ('patti', fcget(fcs, 'patti')),
    ])


def test_scan_all(store, fcs):
    store.put(fcs)
    assert_set_eq(store.scan(), fcs)

    assert list(store.scan_ids()) \
        == list(['big-man', 'boss', 'patti'])


def test_scan_all_random(store):
    import random
    ids = []
    for _ in xrange(100):
        s = ''.join([chr(random.randrange(ord('a'), ord('z') + 1))
                     for _ in xrange(random.randrange(2, 20))])
        ids.append(s)
    ids = list(set(ids))

    assert len(list(store.scan_ids())) == 0
    store.put([(id, FC()) for id in ids])
    assert list(store.scan_ids()) == sorted(ids)


def test_scan_all_weird(store):
    ids = [
        '99bc49e2492a48cb9179d70d3c11ea13',
        'd858432fd50f4cb5a01af290358cb0d1',
        'be75888da4854c15859692b6db590f55',
    ]
    assert len(list(store.scan_ids())) == 0
    store.put([(id, FC()) for id in ids])
    assert list(store.scan_ids()) == sorted(ids)


def test_scan_some(store, fcs):
    store.put(fcs)
    assert_set_eq(store.scan(('b', 'b')),
                  [('boss', fcget(fcs, 'boss')),
                   ('big-man', fcget(fcs, 'big-man'))])


def test_scan_prefix(store, fcs):
    store.put(fcs)
    assert_set_eq(store.scan_prefix('b'),
                  [('boss', fcget(fcs, 'boss')),
                   ('big-man', fcget(fcs, 'big-man'))])

    assert frozenset(store.scan_prefix_ids('b')) \
        == frozenset(['boss', 'big-man'])


def test_delete(store, fcs):
    store.put(fcs)
    store.delete('boss')
    assert len(list(store.scan_ids())) == len(fcs) - 1


def test_delete_all(elastic_address, namespace_string, store, fcs):
    store.put(fcs)
    store.delete_all()
    try:
        store = create_test_store(elastic_address, namespace_string)
        assert len(list(store.scan_ids())) == 0
    finally:
        store.delete_all()


def test_get_missing(store):
    assert store.get('boss') is None


def test_get_many_missing(store):
    assert frozenset(store.get_many(['boss', 'patti'])) \
        == frozenset([('boss', None), ('patti', None)])


def test_get_many_some_missing(store, fcs):
    store.put(fcs)
    store.delete('boss')
    assert_set_eq(store.get_many(['boss', 'patti']),
                  [('boss', None), ('patti', fcget(fcs, 'patti'))])


def test_put_overwrite(store, fcs):
    store.put(fcs)
    newfc = FC({'NAME': {'foo': 1, 'bar': 1}})
    store.put([('boss', newfc)])
    got = store.get('boss')
    assert got == newfc


def test_keyword_scan(store, fcs):
    store.put(fcs)
    # Searching by the boss will connect with big-man because they both
    # have `the` in the `boNAME` feature.
    assert frozenset(store.keyword_scan_ids('boss')) \
        == frozenset(['big-man'])


def test_keyword_scan_partial(store, fcs):
    store.put(fcs)
    assert_set_eq(store.keyword_scan('boss'),
                  [('big-man', fcget(fcs, 'big-man'))])

    expected = FC({
        'NAME': {'Clarence Clemons': 8, 'The Big Man': 1},
    })
    assert_set_eq(store.keyword_scan('boss', feature_names=['NAME']),
                  [('big-man', expected)])


def test_keyword_scan_emphemeral(store, fcs):
    store.put(fcs)
    query_id = 'pattim'

    query_fc = FC({'NAME': {'Patti Mayonnaise': 1}})
    assert frozenset(store.keyword_scan_ids(query_id, query_fc)) \
        == frozenset()

    query_fc['boNAME']['patti'] += 1
    query_fc['boNAME']['mayonnaise'] += 1
    assert frozenset(store.keyword_scan_ids(query_id, query_fc)) \
        == frozenset(['patti'])


def test_optional_indexing(store, fcs):
    store.put(fcs)
    foo1 = FC({
        'NAME': {'Foo Bar': 1},
        'boNAME': {'bruce': 1, 'patti': 1, 'foo': 1, 'bar': 1},
    })
    foo2 = FC({
        'NAME': {'Foo Baz': 1},
        'boNAME': {'foo': 1, 'baz': 1},
    })
    store.put([('foo1', foo1), ('foo2', foo2)], indexes=False)

    # Are they really there?
    assert store.get('foo1') == foo1
    assert store.get('foo2') == foo2

    assert frozenset(store.keyword_scan_ids('foo1')) \
        == frozenset(['boss', 'patti'])
    assert frozenset(store.index_scan_ids('boNAME', 'patti')) \
        == frozenset(['patti'])


def test_index_scan_ids(store, fcs):
    store.put(fcs)
    assert frozenset(store.index_scan_ids('boNAME', 'the')) \
        == frozenset(['boss', 'big-man'])


def test_byte_keys(store):
    fc = FC({'NAME': {'Foo Bar': 1}})
    store.put([('\x00\xff\xf4', fc)])


def test_delete_non_existing_fc(store):
    store.delete('DNE')


def test_scan_ids(store):
    store.put([
        ('a', FC()), ('b', FC()), ('c', FC()), ('d', FC()), ('e', FC()),
        ('f', FC()), ('g', FC()), ('h', FC()), ('i', FC()), ('j', FC()),
        ('k', FC()), ('l', FC()), ('m', FC()), ('n', FC()), ('o', FC()),
    ])
    expected = 'abcdefghijklmno'
    got = ''.join(sorted(store.scan_ids()))
    assert expected == got


def test_index_mapping_keyword(elastic_address, namespace_string, fcs):
    store = ElasticStoreSync(
        hosts=elastic_address, namespace=NAMESPACE, type=namespace_string,
        feature_indexes=[{
            'NAME': {
                'es_index_type': 'string',
                'feature_names': ['NAME', 'boNAME'],
            },
        }, {
            'boNAME': {'es_index_type': 'string', 'feature_names': []},
        }])
    try:
        store.put(fcs)

        query = FC({'NAME': {'The Boss': 1, 'clarence': 1}})
        assert frozenset(store.keyword_scan_ids('ephemeral', query)) \
            == frozenset(['boss', 'big-man'])
    finally:
        store.delete_all()


def test_index_mapping_raw_scan(elastic_address, namespace_string, fcs):
    store = ElasticStoreSync(
        hosts=elastic_address, namespace=NAMESPACE, type=namespace_string,
        feature_indexes=[{
            'NAME': {
                'es_index_type': 'string',
                'feature_names': ['NAME', 'boNAME'],
            },
        }, {
            'boNAME': {'es_index_type': 'string', 'feature_names': []},
        }])
    try:
        store.put(fcs)

        assert frozenset(store.index_scan_ids('NAME', 'The Boss')) \
            == frozenset(['boss'])
        assert frozenset(store.index_scan_ids('NAME', 'clarence')) \
            == frozenset(['big-man'])
    finally:
        store.delete_all()


def test_fulltext_scan(store, fcs_texts):
    store.put(fcs_texts)

    query = FC({u'body': {u'valley': 1}})
    assert frozenset(map(itemgetter(1),
                         store.fulltext_scan_ids(query_fc=query))) \
        == frozenset(['patti'])

    query = FC({u'body': {u'in': 1}})
    assert frozenset(map(itemgetter(1),
                         store.fulltext_scan_ids(query_fc=query))) \
        == frozenset(['patti', 'big-man'])

    query = FC({u'body': {u"mary's": 1}})
    assert frozenset(map(itemgetter(1),
                         store.fulltext_scan_ids(query_fc=query))) \
        == frozenset(['boss'])


def test_fulltext_mapping_keyword(elastic_address, namespace_string,
                                  fcs_texts):
    store = ElasticStoreSync(
        hosts=elastic_address, namespace=NAMESPACE, type=namespace_string,
        fulltext_indexes=[{
            'body': ['body2'],
        }])
    try:
        store.put(fcs_texts)

        query = FC({'body': {'vision': 1}})
        assert frozenset(map(itemgetter(1),
                             store.fulltext_scan_ids(query_fc=query))) \
            == frozenset(['boss'])
    finally:
        store.delete_all()
