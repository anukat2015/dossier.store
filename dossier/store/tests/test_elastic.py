'''Test the ElasticSearch backend.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
import logging
import uuid

import pytest

from dossier.fc import FeatureCollection as FC
from dossier.store.elastic import ElasticStore


logger = logging.getLogger(__name__)


@pytest.yield_fixture  # noqa
def store():
    s = create_test_store()
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


def create_test_store():
    # Give each instantiation its own namespace so that tests don't
    # share mutable global state.
    namespace = str(uuid.uuid4())
    return ElasticStore(
        hosts='172.17.42.1', namespace=namespace,
        feature_indexes=[{
            'NAME': {'es_index_type': 'string', 'feature_names': ['NAME']},
        }, {
            'boNAME': {'es_index_type': 'string', 'feature_names': ['boNAME']},
        }])


def put_fcs(store, fcs):
    store.put(fcs)
    # ES will ACK a put before making it available to a get.
    # Generally speaking, this isn't a huge problem, but for writing tests
    # at least, sync'ing is quite convenient.
    store.sync()


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
    store.sync()
    assert fcboss == store.get('boss')


def test_get_partial(store, fcs):
    put_fcs(store, fcs)
    fc = store.get('boss', feature_names=['NAME'])
    assert 'boNAME' in fcget(fcs, 'boss')
    assert 'boNAME' not in fc


def test_get_many(store, fcs):
    put_fcs(store, fcs)
    assert_set_eq(store.get_many(['boss', 'patti']), [
        ('boss', fcget(fcs, 'boss')),
        ('patti', fcget(fcs, 'patti')),
    ])


def test_scan_all(store, fcs):
    put_fcs(store, fcs)
    assert_set_eq(store.scan(), map(lambda (_, x): x, fcs))

    assert list(store.scan_ids()) \
        == list(['big-man', 'boss', 'patti'])


def test_scan_some(store, fcs):
    put_fcs(store, fcs)
    assert_set_eq(store.scan(('b', 'b')),
                  [fcget(fcs, 'boss'), fcget(fcs, 'big-man')])


def test_scan_prefix(store, fcs):
    put_fcs(store, fcs)
    assert_set_eq(store.scan_prefix('b'),
                  [fcget(fcs, 'boss'), fcget(fcs, 'big-man')])

    assert frozenset(store.scan_prefix_ids('b')) \
        == frozenset(['boss', 'big-man'])


def test_delete(store, fcs):
    put_fcs(store, fcs)
    store.delete('boss')
    store.sync()
    assert len(list(store.scan_ids())) == len(fcs) - 1


def test_delete_all(store, fcs):
    put_fcs(store, fcs)
    store.delete_all()
    try:
        store = create_test_store()
        assert len(list(store.scan_ids())) == 0
    finally:
        store.delete_all()


def test_get_missing(store):
    assert store.get('boss') is None


def test_get_many_missing(store):
    assert frozenset(store.get_many(['boss', 'patti'])) \
        == frozenset([('boss', None), ('patti', None)])


def test_get_many_some_missing(store, fcs):
    put_fcs(store, fcs)
    store.delete('boss')
    store.sync()
    assert_set_eq(store.get_many(['boss', 'patti']),
                  [('boss', None), ('patti', fcget(fcs, 'patti'))])


def test_put_overwrite(store, fcs):
    put_fcs(store, fcs)
    newfc = FC({'NAME': {'foo': 1, 'bar': 1}})
    store.put([('boss', newfc)])
    store.sync()
    got = store.get('boss')
    assert got == newfc


def test_canopy_scan(store, fcs):
    put_fcs(store, fcs)
    # Searching by the boss will connect with big-man because they both
    # have `the` in the `boNAME` feature.
    assert frozenset(store.canopy_scan_ids('boss')) \
        == frozenset(['big-man'])


def test_canopy_scan_partial(store, fcs):
    put_fcs(store, fcs)
    assert_set_eq(store.canopy_scan('boss'),
                  [('big-man', fcget(fcs, 'big-man'))])

    expected = FC({
        'NAME': {'Clarence Clemons': 8, 'The Big Man': 1},
    })
    assert_set_eq(store.canopy_scan('boss', feature_names=['NAME']),
                  [('big-man', expected)])


def test_canopy_scan_emphemeral(store, fcs):
    put_fcs(store, fcs)
    query_id = 'pattim'

    query_fc = FC({'NAME': {'Patti Mayonnaise': 1}})
    assert frozenset(store.canopy_scan_ids(query_id, query_fc)) \
        == frozenset()

    query_fc['boNAME']['patti'] += 1
    query_fc['boNAME']['mayonnaise'] += 1
    assert frozenset(store.canopy_scan_ids(query_id, query_fc)) \
        == frozenset(['patti'])


def test_fc_type(store, fcs):
    put_fcs(store, fcs)
    foo1 = FC({
        'NAME': {'Foo Bar': 1},
        'boNAME': {'bruce': 1, 'patti': 1, 'foo': 1, 'bar': 1},
    })
    foo2 = FC({
        'NAME': {'Foo Baz': 1},
        'boNAME': {'foo': 1, 'baz': 1},
    })
    store.put([('foo1', foo1), ('foo2', foo2)], fc_type='zzz')
    store.sync()

    # Are they really there?
    assert store.get('foo1') == foo1
    assert store.get('foo2') == foo2

    # Make sure id scans respect fc_type.
    assert frozenset(store.scan_ids()) \
        == frozenset(['foo1', 'foo2', 'boss', 'big-man', 'patti'])
    assert frozenset(store.scan_ids(fc_type='fc')) \
        == frozenset(['boss', 'big-man', 'patti'])
    assert frozenset(store.scan_ids(fc_type='zzz')) \
        == frozenset(['foo1', 'foo2'])

    # Make sure prefix id scans respect fc_type.
    assert frozenset(store.scan_prefix_ids('f', fc_type=None)) \
        == frozenset(['foo1', 'foo2'])
    assert frozenset(store.scan_prefix_ids('f', fc_type='fc')) \
        == frozenset()
    assert frozenset(store.scan_prefix_ids('f', fc_type='zzz')) \
        == frozenset(['foo1', 'foo2'])

    # Make sure canopy scans respect fc_type.
    assert frozenset(store.canopy_scan_ids('foo1', fc_type=None)) \
        == frozenset(['boss', 'patti', 'foo2'])
    assert frozenset(store.canopy_scan_ids('foo1', fc_type='fc')) \
        == frozenset(['boss', 'patti'])
    assert frozenset(store.canopy_scan_ids('foo1', fc_type='zzz')) \
        == frozenset(['foo2'])

    # And simple index scans too.
    assert frozenset(store.index_scan('boNAME', 'patti', fc_type=None)) \
        == frozenset(['patti', 'foo1'])
    assert frozenset(store.index_scan('boNAME', 'patti', fc_type='fc')) \
        == frozenset(['patti'])
    assert frozenset(store.index_scan('boNAME', 'patti', fc_type='zzz')) \
        == frozenset(['foo1'])


def test_index_scan(store, fcs):
    put_fcs(store, fcs)
    assert frozenset(store.index_scan('boNAME', 'the')) \
        == frozenset(['boss', 'big-man'])


def test_byte_keys(store):
    fc = FC({'NAME': {'Foo Bar': 1}})
    store.put([('\x00\xff\xf4', fc)])


def test_delete_non_existing_fc(store):
    store.delete('DNE')
