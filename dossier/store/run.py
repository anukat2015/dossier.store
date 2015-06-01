'''dossier.store.run

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import argparse
from functools import partial
from itertools import chain, islice
import json
import urllib
import uuid

import cbor
import kvlayer
import yakonfig

from dossier.fc import FeatureCollection, FeatureCollectionChunk, StringCounter
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
        p.add_argument('--count-features', action='store_true',
                       default=False, help='show count of features')
        p.add_argument('--show-features', action='store_true',
                       default=False, help='show strings from features')
        p.add_argument('--prefix-filter', help='prefix required of all'
                       ' content_ids returned')

    def do_ids(self, args):
        if args.show_features:
            for (cid, fc) in self.store.scan():
                if args.prefix_filter and \
                   not cid.startswith(args.prefix_filter): continue
                print('%r\n%s' % (cid, pretty_string(fc)))
            return
        for cid in self.store.scan_ids():
            if args.prefix_filter and \
               not cid.startswith(args.prefix_filter): continue
            if args.count_features:
                fc = self.store.get(cid)
                print('%d features\t%r' % (len(fc), cid))
            else:
                print(cid)

    def args_get(self, p):
        p.add_argument('content_id', type=str,
                       help='The `content_id` of the feature '
                            'collection to show.')
        p.add_argument('--feature-name', type=str,
                       help='Name of a particular feature to show.')

    def do_get(self, args):
        fc = self.store.get(args.content_id)
        if args.feature_name:
            print(fc.get(args.feature_name))
        else:
            print(fc)

    def args_find(self, p):
        p.add_argument('idx_name', type=str,
                       help='The name of the feautre index.')
        p.add_argument('query', type=str,
                       help='The `query` to find in a feature index '
                            'collection to show.')
        p.add_argument('--show-features', action='store_true',
                       default=False, help='show strings from features')

    def do_find(self, args):
        for cid in self.store.index_scan(args.idx_name, args.query):
            print(cid)
            if args.show_features:
                fc = self.store.get(cid)
                print(pretty_string(fc).encode('utf8'))

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


def pretty_string(fc):
    '''construct a nice looking string for an FC
    '''
    s = []
    for fname, feature in sorted(fc.items()):
        if isinstance(feature, StringCounter):
            feature = [u'%s: %d' % (k, v)
                       for (k,v) in feature.most_common()]
            feature = u'\n\t' + u'\n\t'.join(feature)
        s.append(fname + u': ' + feature)
    return u'\n'.join(s)


def main():
    p = argparse.ArgumentParser(
        description='Interact with the Dossier feature collection store.')
    app = App()
    app.add_arguments(p)
    args = yakonfig.parse_args(p, [kvlayer, yakonfig, Store])
    app.main(args)
