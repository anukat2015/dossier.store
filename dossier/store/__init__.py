'''A simple storage interface for feature collections.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

mod:`dossier.store` provides a convenient interface to a mod:`kvlayer`
table for storing :class:`dossier.fc.FeatureCollection`. The
interface consists of methods to query, search, add and remove feature
collections from the store. It also provides functions for defining and
searching indexes.

An example showing how to store, retrieve and delete a feature
collection:

.. code-block:: python

    fc = dossier.fc.FeatureCollection()
    fc[u'NAME'][u'foo'] += 1
    fc[u'NAME'][u'bar'] = 42

    kvl = kvlayer.client()
    store = dossier.store.Store(kvl)

    store.put('{yourid}', fc)
    assert store.get('{yourid}')[u'NAME'][u'bar'] == 42
    store.delete('{yourid}')
    assert store.get('{yourid}') is None

Here is another example that demonstrates use of indexing to enable a
poor man's case insensitive search:

.. code-block:: python

    fc = dossier.fc.FeatureCollection()
    fc[u'NAME'][u'foo'] += 1
    fc[u'NAME'][u'bar'] = 42

    kvl = kvlayer.client()
    store = dossier.store.Store(kvl)

    # Index transforms must be defined on every instance of `Store`.
    # (The index data is persisted; the transforms themselves are
    # ephemeral.)
    store.define_index(u'name_casei',
                       create=feature_index(u'NAME'),
                       transform=lambda s: s.lower().encode('utf-8'))

    store.put('{yourid}', fc)  # `put` automatically updates indexes.
    assert list(store.index_scan(u'name_casei', 'FoO'))[0] == '{yourid}'

.. autoclass:: Store
.. autofunction:: feature_index
'''
from dossier.store.store import Store, feature_index

__all__ = ['Store', 'feature_index']
