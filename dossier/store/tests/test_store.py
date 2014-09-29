#!/usr/bin/env python
'''dossier.store

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
import itertools
import logging

import pytest

from dossier.fc import FeatureCollection
from dossier.store import FCStorage, Content, content_type, feature_index

from dossier.store.tests import kvl


logger = logging.getLogger(__name__)


class PlainStorage (FCStorage):
    @staticmethod
    def encode(content):
        return ((content.content_id,), content.data)

    @staticmethod
    def decode(key_val):
        key, data = key_val
        return Content(key[0], data)

    def __init__(self, *args, **kwargs):
        super(PlainStorage, self).__init__(*args, **kwargs)
        self.add_index('data',
                       lambda t, c: itertools.chain([t(c.data)]),
                       lambda data: data.upper())


@pytest.fixture
def plain_store(kvl):
    return PlainStorage(kvl)


@pytest.fixture
def fcstore(kvl):
    return FCStorage(kvl)


def test_content_type():
    assert content_type('kb_wat') == 'kb'


def test_content_id_scan(plain_store):
    a = Content(content_id='aA', data='x')
    b = Content(content_id='aB', data='y')
    c = Content(content_id='bC', data='z')
    plain_store.put(a, b, c)

    ids = list(plain_store.scan_prefix('a', only_ids=True))
    assert 2 == len(ids)
    assert all(map(lambda id: isinstance(id, str), ids))


def test_custom_indexes(plain_store):
    a = Content(content_id='aA', data='xx')
    b = Content(content_id='aB', data='xy')
    c = Content(content_id='bC', data='zz')
    d = Content(content_id='bD', data='xx')
    plain_store.put(a, b, c, d, indexes=True)

    assert 2 == len(list(plain_store.index_scan('data', 'xx')))
    assert 2 == len(list(plain_store.index_scan('data', 'xX')))
    assert 2 == len(list(plain_store.index_scan('data', 'Xx')))
    assert 2 == len(list(plain_store.index_scan('data', 'XX')))

    assert 3 == len(list(plain_store.index_scan_prefix('data', 'x')))
    assert 3 == len(list(plain_store.index_scan_prefix('data', 'X')))


def mk_fc_names(*names):
    assert len(names) > 0
    feat = FeatureCollection()
    feat['canonical_name'][names[0]] = 1
    for name in names:
        feat['NAME'][name] += 1
    return feat


def test_fcs(fcstore):
    feata = mk_fc_names('foo', 'baz')
    a = Content(content_id='a', data=feata)
    fcstore.put(a)
    assert fcstore.get('a').data == feata


def test_fcs_index(fcstore):
    fcstore.add_index('NAME',
                      feature_index('NAME'),
                      lambda s: s.lower().encode('utf-8'))
    feata = mk_fc_names('foo', 'baz')
    fcstore.put(Content(content_id='a', data=feata), indexes=True)
    assert list(fcstore.index_scan('NAME', 'FoO'))[0] == 'a'
    assert list(fcstore.index_scan('NAME', 'bAz'))[0] == 'a'
    assert list(fcstore.index_scan_prefix('NAME', 'b'))[0] == 'a'


def test_fcs_index_only_canonical(fcstore):
    fcstore.add_index('NAME',
                      feature_index('canonical_name'),
                      lambda s: s.lower().encode('utf-8'))
    feata = mk_fc_names('foo', 'baz')
    fcstore.put(Content(content_id='a', data=feata), indexes=True)
    assert list(fcstore.index_scan('NAME', 'FoO'))[0] == 'a'
    assert len(list(fcstore.index_scan('NAME', 'bAz'))) == 0


def test_fcs_index_raw(fcstore):
    fcstore.add_index('NAME',
                      feature_index('NAME'),
                      lambda s: s.lower().encode('utf-8'))
    feata = mk_fc_names('foo', 'baz')
    fcstore.put(Content(content_id='a', data=feata), indexes=False)

    assert len(list(fcstore.index_scan('NAME', 'FoO'))) == 0
    assert len(list(fcstore.index_scan('NAME', 'bAz'))) == 0

    fcstore._index_put_raw('NAME', 'a', 'foo')
    fcstore._index_put_raw('NAME', 'a', 'baz')
    assert list(fcstore.index_scan('NAME', 'FoO'))[0] == 'a'
    assert list(fcstore.index_scan('NAME', 'bAz'))[0] == 'a'
    assert list(fcstore.index_scan_prefix('NAME', 'b'))[0] == 'a'
