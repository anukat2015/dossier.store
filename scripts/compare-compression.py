'''simple diagnostic script for comparing the compression ratios and
speed of different compression tools, which is useful for working with
FeatureCollections.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import division, print_function
import argparse
import cStringIO
import sys
import time

## this are in the python 2.7 standard library
import gzip as gz
import bz2

## This is in python3, so we get it from a backport that requires
## liblzma-dev to be installed locally, so the C header files are
## available
from backports import lzma as xz

## This requires libsnappy-dev for C header files
import snappy as sz


parser = argparse.ArgumentParser()
parser.add_argument('path', help='path to an XZ-compressed file to read for compression tests')
args = parser.parse_args()

## could check something about the FCs
#from dossier.fc.feature_collection import FeatureCollectionChunk as FCChunk
#fcc = FCChunk(path)
#for fc in fcc:
#    print fc['feature_name']


## assume the incoming data is some long-term archival stuff in XZ
## compressed format, and you want to see how slow XZ is:
xz_data = open(args.path).read()
start = time.time()
data = xz.decompress(xz_data)
decompression_time = time.time() - start

start = time.time()
xz_data2 = xz.compress(data)
assert xz_data2 == xz_data
compression_time = time.time() - start

def report(rec):
    rec['MB'] = rec['bytes'] / 2**20
    rec['ratio'] = rec['bytes'] / len(data)
    ctime = rec.get('compression_time')
    rec['compression_rate'] = ctime and rec['MB'] / ctime or float('inf')
    dtime = rec.get('decompression_time')
    rec['decompression_rate'] = dtime and rec['MB'] / dtime or 0
    print('%(name)s:\t%(MB)d MB of FC data, %(ratio).3f compression, %(compression_time).3f seconds --> %(compression_rate).3f MB/sec compression, '
          '%(decompression_time).3f seconds --> %(decompression_rate).3f MB/sec decompression' % rec)
    sys.stdout.flush()

raw_rec = dict(name='raw', bytes=len(data), compression_time=0, decompression_time=0)
report(raw_rec)

xz_rec = dict(name='xz', bytes=len(xz_data), compression_time=compression_time, decompression_time=decompression_time)
report(xz_rec)
del xz_data, xz_data2

#### gz
## compress
start = time.time()
fh = cStringIO.StringIO()
gz_fh = gz.GzipFile(fileobj=fh, mode='w')
gz_fh.write(data)
gz_fh.close()
gz_data = fh.getvalue()
compression_time = time.time() - start
## decompress
start = time.time()
fh = cStringIO.StringIO(gz_data)
gz_fh = gz.GzipFile(fileobj=fh, mode='r')
data2 = gz_fh.read(data)
gz_fh.close()
decompression_time = time.time() - start
assert data2 == data
gz_rec = dict(name='gz', bytes=len(gz_data), compression_time=compression_time, decompression_time=decompression_time)
report(gz_rec)
del gz_data, fh, gz_fh, data2

## bz2
start = time.time()
bz2_data = bz2.compress(data)
compression_time = time.time() - start
start = time.time()
data2 = bz2.decompress(bz2_data)
decompression_time = time.time() - start
assert data2 == data
bz2_rec = dict(name='bz2', bytes=len(bz2_data), compression_time=compression_time, decompression_time=decompression_time)
report(bz2_rec)
del bz2_data, data2

## snappy!
start = time.time()
sz_data = sz.compress(data)
compression_time = time.time() - start
start = time.time()
data2 = sz.decompress(sz_data)
compression_time = time.time() - start
assert data2 == data
sz_rec = dict(name='snappy',  bytes=len(sz_data), compression_time=compression_time, decompression_time=decompression_time)
report(sz_rec)

print('# done.')
