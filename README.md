`dossier.store` is a Python package that provides database storage for
feature collections in DossierStack. It is built on top of `kvlayer`, so
it supports many database backends like PostgreSQL, Accumulo and Redis.
`dossier.store` also provides support for defining basic indexes.


### Installation

`dossier.store` is on PyPI and can be installed with `pip`:

```bash
pip install dossier.store
```

Currently, `dossier.store` requires Python 2.7. It is not yet Python 3
compatible.


### Documentation

API documentation with examples is available as part of the Dossier Stack
documentation:
[http://dossier-stack.readthedocs.org](http://dossier-stack.readthedocs.org#module-dossier.store)


### Basic example

A full working example that uses local memory to store a feature collection:

```python
from dossier.fc import FeatureCollection
from dossier.store import Store
import kvlayer
import yakonfig

yaml = """
kvlayer:
  app_name: store
  namespace: dossier
  storage_type: local
"""
with yakonfig.defaulted_config([kvlayer], yaml=yaml):
    store = Store(kvlayer.client())

    fc = FeatureCollection({u'NAME': {'Foo': 1, 'Bar': 2}})
    store.put([('1', fc)])
    print store.get('1')
```


### Example using indexes

Here is another example that demonstrates use of indexing to enable a
poor man's case insensitive search:

```python
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
```

