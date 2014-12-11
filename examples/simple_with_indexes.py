from dossier.fc import FeatureCollection, StringCounter
from dossier.store import Store
import kvlayer

# Uses a configuration-free in memory database. This is ONLY useful for
# development, debugging or testing.
conn = kvlayer.client(config={}, storage_type='local')

# Or you can use this to test with Redis.
# config = {
    # 'storage_type': 'redis',
    # 'storage_addresses': ['localhost:6379'],
    # 'app_name': 'your-app-name',
    # 'namespace': 'features',
# }
# conn = kvlayer.client(config=config)

# Use something like this for HBase.
# config = {
    # 'storage_type': 'hbase',
    # 'storage_addresses': ['127.0.0..1:17111'],
    # 'username': 'username',
    # 'password': 'password',
    # 'dbname': 'database-name',
    # 'app_name': 'your-app-name',
    # 'namespace': 'features',
# }
# conn = kvlayer.client(config=config)

# There are more backends available like MySQL, PostgreSQL and Accumulo.
#
# See: https://github.com/diffeo/kvlayer

# !!! IMPORTANT !!!
# Define features that you want to index. This will let you quickly scan
# for feature collections in the database with matching values.
#
# You don't have to index everything, but it's probably a good idea to index
# the most prominent features. e.g., phone or email or website.
#
# These should correspond to the names of the corresponding features.
feature_indexes = [u'phone', u'email', u'website', u'rate']

# Create a "store," which knows how to store and index feature collections.
store = Store(conn, feature_indexes=feature_indexes)

# Create a fresh feature collection and add a 'rate' feature.
fc = FeatureCollection()
fc['rate'] = StringCounter({
    u'5per30': 5,
    u'5per60': 1,
    u'10per20': 2,
})

# Content ids are the unique identifier for each feature collection.
# It's probably sufficient to use whatever you have for "ad id."
content_id = 'some_unique_value'
store.put([(content_id, fc)])
print store.get(content_id)

# Use the index scan!
print list(store.index_scan_prefix(u'rate', '10'))
