'''dossier.store.run

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import argparse
import uuid

import kvlayer
import yakonfig

from dossier.fc import FeatureCollectionChunk
from dossier.store import Store


class App(yakonfig.cmd.ArgParseCmd):
    def __init__(self, *args, **kwargs):
        yakonfig.cmd.ArgParseCmd.__init__(self, *args, **kwargs)
        self._store = None

    @property
    def store(self):
        if self._store is None:
            self._store = Store(kvlayer.client())
        return self._store

    def args_load(self, p):
        p.add_argument('chunk_files', nargs='+',
                       help='One or more feature collection chunk files.')
        p.add_argument('--id-feature', default=None,
                       help='The name of the feature containing an id.')

    def do_load(self, args):
        for chunkfile in args.chunk_files:
            for fc in FeatureCollectionChunk(path=chunkfile):
                if args.id_feature is not None:
                    if args.id_feature not in fc:
                        raise KeyError(args.id_feature)
                    content_id = fc[args.id_feature].encode('utf-8')
                else:
                    content_id = str(uuid.uuid4())
                self.store.put([(content_id, fc)])

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


def main():
    p = argparse.ArgumentParser(
        description='Interact with the Dossier feature collection store.')
    app = App()
    app.add_arguments(p)
    args = yakonfig.parse_args(p, [kvlayer, yakonfig])
    app.main(args)
