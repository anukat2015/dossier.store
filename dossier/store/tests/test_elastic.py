'''Test the ElasticSearch backend.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
import logging

import pytest

from dossier.store.elastic import ElasticStore

from dossier.store.tests import kvl  # noqa


logger = logging.getLogger(__name__)


@pytest.fixture  # noqa
def store():
    return ElasticStore()


def test_basic(store):
    print(store.conn.indices.get_settings(store.index))
    assert False
