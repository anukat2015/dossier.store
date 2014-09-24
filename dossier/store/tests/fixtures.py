from __future__ import absolute_import, division, print_function
import contextlib

import pytest

import kvlayer
import yakonfig


@pytest.fixture
def configurator(namespace_string, redis_address, tmpdir):
    '''
    Get a function to create a sane configuration.

    The value of this fixture is actually itself a function which
    is a context manager.  Run your test inside a "with" block,
    for instance:

    >>> def my_test(configurator):
    ...   with configurator():
    ...     tcf = TreeComponentFactory()
    ...     tcf.configure()
    ...     assert tcf._storage is not None

    Alternatively, you may use the `config_local` fixture for a plain
    local storage configuration.

    This defaults to using in-memory storage, but you can pass
    ``storage_type='redis'`` to use a different backend. The
    configuration is validated unless ``validate=False`` disables this.

    This version of the configurator loads :mod:`kvlayer`, and (if
    available) :mod:`streamcorpus_pipeline` configuration. Any
    other keyword parameters are passed into :mod:`yakonfig` as a
    user-provided configuration dictionary which overrides defaults.
    '''
    @contextlib.contextmanager
    def make_config(storage_type='local', validate=True, **kwargs):
        modules = [kvlayer]
        try:
            import streamcorpus_pipeline
            modules.append(streamcorpus_pipeline)
        except ImportError:
            pass
        with yakonfig.defaulted_config(
                modules,
                params={
                    'namespace': namespace_string,
                    'app_name': 'diffeo_test',
                    'registry_addresses': [redis_address],
                    'storage_type': storage_type,
                    'storage_addresses': [redis_address],
                    'tmp_dir_path': str(tmpdir),
                },
                validate=validate,
                config=kwargs) as config:
            yield config
    return make_config


@pytest.yield_fixture
def config_local(configurator):
    '''A totally plain default configuration.'''
    with configurator(storage_type='local'):
        yield


@pytest.yield_fixture
def kvl(config_local):
    client = kvlayer.client()
    yield client
    client.delete_namespace()
