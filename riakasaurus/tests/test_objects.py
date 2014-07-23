#!/usr/bin/env python
"""
riakasaurus trial test file.
riakasaurus _must_ be on your PYTHONPATH

"""

import json
import random
from twisted.trial import unittest
from twisted.python import log
from twisted.internet import defer

VERBOSE = False

from riakasaurus import riak

# uncomment to activate logging
# import sys
# log.startLogging(sys.stderr)

RIAK_CLIENT_ID = 'TEST'
BUCKET_PREFIX = 'riakasaurus.tests.'
JAVASCRIPT_SUM = """
function(v) {
  x = v.reduce(function(a,b){ return a + b }, 0);
  return [x];
}
"""


def randint():
    """Generate nice random int for our test."""
    return random.randint(1, 999999)


class Tests(unittest.TestCase):
    """
    trial unit tests.
    """

    test_keys = ['foo', 'foo1', 'foo2', 'foo3', 'bar', 'baz', 'ba_foo1',
                 'blue_foo1']

    @defer.inlineCallbacks
    def setUp(self):
        self.client = riak.RiakClient(client_id=RIAK_CLIENT_ID)
        self.bucket_name = BUCKET_PREFIX + self.id().rsplit('.', 1)[-1]
        self.bucket = self.client.bucket(self.bucket_name)
        yield self.bucket.enable_search()
        yield self.bucket.purge_keys()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.bucket.disable_search()
        yield self.bucket.purge_keys()

    @defer.inlineCallbacks
    def test_set_data_empty(self):
        """Get an object that does not exist, then set_data and save it """
        log.msg("*** set_data_empty")

        obj = yield self.bucket.get("foo1")

        self.assertEqual(obj.exists(), False)
        self.assertEqual(obj.get_data(), None)

        obj.set_data('bar1')
        yield obj.store()

        self.assertEqual(obj.exists(), True)
        # We get unicode from .get_data() because the content type is JSON.
        self.assertEqual(obj.get_data(), u"bar1")
        log.msg("done set_data_empty")

    @defer.inlineCallbacks
    def test_add_and_delete(self):
        """Basic adds and deletes"""
        log.msg("*** add_and_delete")

        obj = self.bucket.new("foo1", "test1")
        yield obj.store()

        self.assertEqual(obj.exists(), True)
        # We get unicode from .get_data() because the content type is JSON.
        self.assertEqual(obj.get_data(), u"test1")

        obj.set_data('bar1')
        yield obj.store()

        obj = yield self.bucket.get("foo1")
        self.assertEqual(obj.exists(), True)
        # We get unicode from .get_data() because the content type is JSON.
        self.assertEqual(obj.get_data(), u"bar1")

        yield obj.delete()

        obj = yield self.bucket.get("foo1")
        self.assertEqual(obj.exists(), False)
        log.msg("done add_and_delete")

    @defer.inlineCallbacks
    def test_list_keys(self):
        """Test listing all keys in bucket."""
        log.msg("*** list_keys")

        obj = self.bucket.new("foo1", "test1")
        yield obj.store()
        obj1 = self.bucket.new("foo2", "test2")
        yield obj1.store()

        keys = yield self.bucket.list_keys()
        self.assertEqual(["foo1", "foo2"], sorted(keys))

    @defer.inlineCallbacks
    def test_purge_keys(self):
        """Test purging all keys in a bucket."""
        log.msg("*** purge_keys")

        obj = self.bucket.new("foo1", "test1")
        yield obj.store()
        obj1 = self.bucket.new("foo2", "test2")
        yield obj1.store()

        yield self.bucket.purge_keys()

        # FIXME: nasty hack to work around purge_keys returning
        # too soon (or maybe list_keys being weird).
        import time
        start = time.time()
        while True:
            keys = yield self.bucket.list_keys()
            if not keys or time.time() - start > 10:
                break
            time.sleep(0.2)
        self.assertEqual([], keys)

    @defer.inlineCallbacks
    def test_store_and_get(self):
        """Store and get text data."""
        log.msg('*** store_and_get')
        data = 'blueprint'
        obj = self.bucket.new('blue_foo1', data)
        yield obj.store()
        del obj

        obj1 = yield self.bucket.get('blue_foo1')
        self.assertEqual(obj1.exists(), True)
        self.assertEqual(obj1.get_bucket().get_name(), self.bucket_name)
        self.assertEqual(obj1.get_key(), 'blue_foo1')
        # We get unicode from .get_data() because the content type is JSON.
        self.assertEqual(obj1.get_data(), u'blueprint')
        log.msg('done store_and_get')

    @defer.inlineCallbacks
    def test_binary_store_and_get(self):
        """store and get binary data."""

        log.msg('*** binary_store_and_get')

        # Store as binary, retrieve as binary, then compare...
        rand = str(randint())
        obj = self.bucket.new_binary('foo1', rand)
        yield obj.store()
        del obj

        obj = yield self.bucket.get_binary('foo1')
        self.assertEqual(obj.exists(), True)
        self.assertEqual(obj.get_data(), rand)
        del obj

        # Store as JSON, retrieve as binary, JSON-decode, then compare...
        data = [randint(), randint(), randint()]
        obj = self.bucket.new('foo2', data)
        yield obj.store()
        del obj

        obj = yield self.bucket.get_binary('foo2')
        self.assertEqual(data, json.loads(obj.get_data()))
        log.msg('done binary_store_and_get')

    @defer.inlineCallbacks
    def test_missing_object(self):
        """handle missing objects."""
        log.msg('*** missing_object')
        obj = yield self.bucket.get("missing")
        self.assertEqual(not obj.exists(), True)
        self.assertEqual(obj.get_data(), None)
        log.msg('done missing_object')

    @defer.inlineCallbacks
    def test_delete(self):
        """delete objects"""
        log.msg('*** delete')
        rand = randint()
        obj = self.bucket.new('foo', rand)
        yield obj.store()
        obj = yield self.bucket.get('foo')
        self.assertEqual(obj.exists(), True)
        yield obj.delete()
        yield obj.reload()
        self.assertEqual(obj.exists(), False)
        log.msg('done delete')

    @defer.inlineCallbacks
    def test_siblings(self):
        """find siblings"""
        # Siblings works except for the store at the end.
        # Need to isolate and test that separately.
        log.msg('*** siblings')

        # Set up the bucket, clear any existing object...
        yield self.bucket.set_allow_multiples(True)
        obj = yield self.bucket.get('foo')
        yield obj.delete()

        obj = yield self.bucket.get('foo')
        self.assertEqual(obj.exists(), False)

        # Store the same object multiple times...
        for i in range(5):
            # for this to work, we must get a new RiakClient
            # on each pass and it must have a different client_id.
            # calling RiakClient without params uses randomly-generate id.
            client = riak.RiakClient()
            bucket = client.bucket(self.bucket_name)
            yield bucket.new('foo', randint()).store()

        # Make sure the object has 5 siblings...
        yield obj.reload()
        self.assertEqual(obj.has_siblings(), True)
        self.assertEqual(obj.get_sibling_count(), 5)

        # Test get_sibling()/get_siblings()...
        siblings = yield obj.get_siblings()
        obj3 = yield obj.get_sibling(3)

        self.assertEqual(siblings[3].get_data(), obj3.get_data())

        # Resolve the conflict, and then do a get...
        obj3 = yield obj.get_sibling(3)
        yield obj3.store()
        yield obj.reload()
        self.assertEqual(obj.get_data(), obj3.get_data())

        # Clean up for next test...
        yield obj.delete()
        log.msg('done siblings')

    @defer.inlineCallbacks
    def test_store_and_get_links(self):
        """manipulate links"""
        # if we store with the basho test, we get this correctly.
        # so there's now something wrong with link storage.
        log.msg('*** store_and_get_links')

        quote_string = "tag2!@#%^&*)"

        obj = self.bucket.new("foo", 2) \
                .add_link(self.bucket.new("foo1")) \
                .add_link(self.bucket.new("foo2"), "tag") \
                .add_link(self.bucket.new(quote_string), quote_string)
        yield obj.store()
        del obj

        log.msg("Get the Links")
        obj = yield self.bucket.get("foo")
        links = obj.get_links()
        self.assertEqual(len(links), 3)

        quote_link = links[-1]
        self.assertEqual(quote_link.get_key(), quote_string)
        self.assertEqual(quote_link.get_tag(), quote_string)

        log.msg('done store_and_get_links')

    @defer.inlineCallbacks
    def test_link_walking(self):
        """walk links"""
        log.msg('*** link_walking')
        obj_1 = self.bucket.new("foo1", "test1")
        yield obj_1.store()

        obj_2 = self.bucket.new("foo2", "test2")
        yield obj_2.store()

        obj_3 = self.bucket.new("foo3", "test3")
        yield obj_3.store()

        obj = self.bucket.new("foo", 2) \
                .add_link(obj_1) \
                .add_link(obj_2, "tag") \
                .add_link(obj_3, "tag2!@#%^&*)")
        yield obj.store()
        obj = yield self.bucket.get("foo")
        job = obj.link(self.bucket_name)
        results = yield job.run()
        self.assertEqual(len(results), 3)
        results = yield obj.link(self.bucket_name, "tag").run()
        self.assertEqual(len(results), 1)
        log.msg('done link_walking')

    @defer.inlineCallbacks
    def test_meta_data_simple(self):
        """ensure we can get and set metadata"""
        log.msg('*** meta_data_simple')

        key = "foo1"
        key_data = "test1"

        # be sure object is deleted before we start
        obj = yield self.bucket.get(key)
        yield obj.delete()

        # now get a fresh new one
        obj = self.bucket.new(key, key_data)

        # see we can store and get back a header
        meta_key = 'this-is-a-test'
        meta_key1 = 'Fifty-Three'
        meta_data = 'ABCDEFG 123'
        obj.add_meta_data(meta_key, meta_data)
        metas = obj.get_all_meta_data()
        self.assertTrue(meta_key in metas)
        self.assertEqual(1, len(metas))

        # add second meta_data and get back both.
        # since they are stored as dictionary, we cannot
        # trus the order
        obj.add_meta_data(meta_key1, meta_data)
        metas = obj.get_all_meta_data()
        self.assertEqual(2, len(metas))
        self.assertTrue(meta_key in metas)
        self.assertTrue(meta_key1 in metas)

        # now delete one of the metas
        obj.remove_meta_data(meta_key)
        metas = obj.get_all_meta_data()
        self.assertFalse(meta_key in metas)
        self.assertTrue(meta_key1 in metas)
        self.assertEqual(1, len(metas))

        # store the object
        # since return body is true, the obj is refreshed
        yield obj.store()
        metas = obj.get_all_meta_data()

        # remember: twisted lowercases the headers!
        mk1_lc = meta_key1.lower()
        self.assertTrue(mk1_lc in metas)
        self.assertTrue(metas[mk1_lc] == meta_data)
        log.msg('done meta_data_simple')
