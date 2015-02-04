'''dossier.store.run

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import argparse
from functools import partial
from itertools import chain, islice
import urllib
import uuid

import cbor
import kvlayer
import yakonfig

from dossier.fc import FeatureCollection, FeatureCollectionChunk
from dossier.store import Store


class App(yakonfig.cmd.ArgParseCmd):
    def __init__(self, *args, **kwargs):
        yakonfig.cmd.ArgParseCmd.__init__(self, *args, **kwargs)
        self._store = None

    @property
    def store(self):
        if self._store is None:
            feature_indexes = None
            try:
                conf = yakonfig.get_global_config('dossier.store')
                feature_indexes = conf['feature_indexes']
            except KeyError:
                pass
            self._store = Store(kvlayer.client(),
                                feature_indexes=feature_indexes)
        return self._store

    def args_load(self, p):
        p.add_argument('chunk_files', nargs='+',
                       help='One or more feature collection chunk files.')
        p.add_argument('--id-feature', default=None,
                       help='The name of the feature containing an id.')
        p.add_argument('--id-feature-prefix', default='',
                       help='Add a prefix to the corresponding id.')
        p.add_argument('--batch-size', default=30, type=int,
                       help='The number of FCs to insert at a time.')

    def do_load(self, args):
        get_content_id = partial(
            self.get_content_id, args.id_feature_prefix, args.id_feature)
        for chunkfile in args.chunk_files:
            if not chunkfile.endswith('.fc'):
                fc_chunker = FeatureCollectionChunk(path=chunkfile)
                for i, fcs in enumerate(chunks(args.batch_size, fc_chunker)):
                    fcs = list(fcs)
                    content_ids = map(get_content_id, fcs)
                    self.store.put(zip(content_ids, fcs))
                    print('batch %d (%d FCs)' % (i, len(fcs)))
            else:
                # This currently seg faults.
                fh = open(chunkfile, 'rb')
                fc_chunker = cbor_iter(fh)
                for i, fcs in enumerate(chunks(args.batch_size, fc_chunker)):
                    fcs = list(fcs)
                    content_ids = map(get_content_id, fcs)
                    self.store.put(zip(content_ids, fcs))
                    print('batch %d (%d FCs)' % (i, len(fcs)))

    def load_one_fc(self, id_prefix, id_feature, fc):
        content_id = self.get_content_id(id_prefix, id_feature, fc)
        self.store.put([(content_id, fc)])

    def get_content_id(self, id_prefix, id_feature, fc):
        cid = None
        if id_feature is None:
            cid = str(uuid.uuid4())
        else:
            if id_feature not in fc:
                raise KeyError(id_feature)
            feat = fc[id_feature]
            if isinstance(feat, unicode):
                cid = feat.encode('utf-8')
            else:
                assert len(feat.keys()) == 1
                cid = feat.keys()[0].encode('utf-8')
        return id_prefix + cid

    def args_ids(self, p):
        pass

    def do_ids(self, args):
        for id in self.store.scan_ids():
            print(id)

    def args_get(self, p):
        p.add_argument('content_id', type=str,
                       help='The `content_id` of the feature '
                            'collection to show.')

    def do_get(self, args):
        print(self.store.get(args.content_id))

    def args_delete_all(self, p):
        pass

    def do_delete_all(self, args):
        self.store.delete_all()


def chunks(n, iterable):
    iterable = iter(iterable)
    while True:
        yield chain([next(iterable)], islice(iterable, n-1))


def cbor_iter(fh):
    while True:
        try:
            chunk = cbor.load(fh)
        except EOFError:
            break
        yield FeatureCollection.from_dict(chunk)


def main():
    p = argparse.ArgumentParser(
        description='Interact with the Dossier feature collection store.')
    app = App()
    app.add_arguments(p)
    args = yakonfig.parse_args(p, [kvlayer, yakonfig, Store])
    app.main(args)
