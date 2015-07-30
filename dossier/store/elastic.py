'''A native ElasticSearch implementation for dossier.store.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import logging

from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


class ElasticStore(object):
    def __init__(self, hosts=None, namespace='d01', feature_indexes=None):
        self.conn = Elasticsearch(hosts=hosts)
        self.index = '%s_fcs' % namespace
        self.type = 'fc'

        if not self.conn.indices.exists(index=self.index):
            # This can race, but that should be OK.
            self.init()

    def init(self):
        self.conn.indices.create(index=self.index)
        self.conn.indices.put_mapping(
            index=self.index, doc_type=self.type, body={
                'dynamic_templates': [{
                    'default_no_analyze': {
                        'match': '*',
                        'mapping': {'index': 'not_analyzed'},
                    },
                }],
            })
