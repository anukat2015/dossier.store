'''dossier.store.tests

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function

import pytest

import kvlayer
import yakonfig


@pytest.yield_fixture
def kvl():
    config = {
        'storage_type': 'local',
        'app_name': 'diffeo',
        'namespace': 'dossier.store.test',
    }
    with yakonfig.defaulted_config([kvlayer], params=config) as config:
        client = kvlayer.client()
        yield client
        client.delete_namespace()
