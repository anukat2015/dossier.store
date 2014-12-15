'''dossier.store.tests.test_store

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
import logging

import pytest

import kvlayer

from dossier.fc import FeatureCollection
from dossier.store import Store, feature_index

from dossier.store.tests import kvl


logger = logging.getLogger(__name__)


@pytest.fixture
def fcstore(kvl):
    return Store(kvl)


def mk_fc_names(*names):
    assert len(names) > 0
    feat = FeatureCollection()
    feat[u'canonical_name'][names[0]] = 1
    for name in names:
        feat[u'NAME'][name] += 1
    return feat


def test_content_id_scan(fcstore):
    fcstore.put([('aA', mk_fc_names('x'))])
    fcstore.put([('aB', mk_fc_names('y'))])
    fcstore.put([('bC', mk_fc_names('z'))])

    ids = list(fcstore.scan_prefix_ids('a'))
    assert 2 == len(ids)
    assert all(map(lambda cid: isinstance(cid, str), ids))


def test_fcs(fcstore):
    feata = mk_fc_names('foo', 'baz')
    fcstore.put([('a', feata)])
    assert fcstore.get('a') == feata


def test_fcs_index(fcstore):
    fcstore.define_index(u'NAME',
                         feature_index('NAME'),
                         lambda s: s.lower().encode('utf-8'))
    feata = mk_fc_names('foo', 'baz')
    fcstore.put([('a', feata)], indexes=True)
    assert list(fcstore.index_scan(u'NAME', 'FoO'))[0] == 'a'
    assert list(fcstore.index_scan(u'NAME', 'bAz'))[0] == 'a'
    assert list(fcstore.index_scan_prefix(u'NAME', 'b'))[0] == 'a'


def test_fcs_bad_unicode_index(fcstore):
    fcstore.define_index(u'NAME',
                         feature_index('NAME'),
                         lambda s: unicode(s.lower()))
    feata = mk_fc_names('foo', 'baz')
    with pytest.raises(kvlayer.BadKey):
        fcstore.put([('a', feata)], indexes=True)


def test_fcs_index_only_canonical(fcstore):
    fcstore.define_index(u'NAME',
                         feature_index('canonical_name'),
                         lambda s: s.lower().encode('utf-8'))
    feata = mk_fc_names('foo', 'baz')
    fcstore.put([('a', feata)], indexes=True)
    assert list(fcstore.index_scan(u'NAME', 'FoO'))[0] == 'a'
    assert len(list(fcstore.index_scan(u'NAME', 'bAz'))) == 0


def test_fcs_index_raw(fcstore):
    fcstore.define_index(u'NAME',
                         feature_index('NAME'),
                         lambda s: s.lower().encode('utf-8'))
    feata = mk_fc_names('foo', 'baz')
    fcstore.put([('a', feata)], indexes=False)

    assert len(list(fcstore.index_scan(u'NAME', 'FoO'))) == 0
    assert len(list(fcstore.index_scan(u'NAME', 'bAz'))) == 0

    fcstore._index_put_raw(u'NAME', 'a', 'foo')
    fcstore._index_put_raw(u'NAME', 'a', 'baz')
    assert list(fcstore.index_scan(u'NAME', 'FoO'))[0] == 'a'
    assert list(fcstore.index_scan(u'NAME', 'bAz'))[0] == 'a'
    assert list(fcstore.index_scan_prefix(u'NAME', 'b'))[0] == 'a'


def test_index_order(fcstore):
    fcstore.define_index(u'a', None, None)
    fcstore.define_index(u'z', None, None)
    fcstore.define_index(u'd', None, None)
    fcstore.define_index(u'c', None, None)
    assert fcstore.index_names() == ['a', 'z', 'd', 'c']
