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
    def test_head(self):
        """create a object, and retrieve metadata via head(), no content is loaded"""
        log.msg("*** head")

        obj = self.bucket.new("foo1", "test1")
        yield obj.store()

        obj = yield self.bucket.head("foo1")
        self.assertEqual(obj.exists(), True)
        self.assertEqual(obj.get_data(), None)

        log.msg("done head")

    @defer.inlineCallbacks
    def test_list_buckets(self):
        """Test listing all buckets."""
        log.msg("*** list_buckets")

        obj1 = self.bucket.new("foo1", "test1")
        yield obj1.store()

        bucket2_name = "%s2" % self.bucket_name
        bucket2 = self.client.bucket(bucket2_name)
        obj2 = bucket2.new("foo2", "test2")
        yield obj2.store()

        buckets = yield self.client.list_buckets()
        # just check that these two buckets exist in case
        # there are buckets not related to these tests
        self.assertTrue(self.bucket_name in buckets)
        self.assertTrue(bucket2_name in buckets)

        # Cleanup after ourselves
        yield obj1.delete()
        yield obj2.delete()

    @defer.inlineCallbacks
    def test_is_alive(self):
        """Can we ping the riak server."""
        log.msg('*** is_alive')
        client_id = self.client.get_client_id()
        self.assertEqual(client_id, RIAK_CLIENT_ID)
        alive = yield self.client.is_alive()
        self.assertEqual(alive, True)
        log.msg('done is_alive')

    @defer.inlineCallbacks
    def test_set_bucket_properties(self):
        """manipulate bucket properties"""
        log.msg('*** set_bucket_properties')
        # Test setting allow mult...
        yield self.bucket.set_allow_multiples(True)
        is_multiples = yield self.bucket.get_allow_multiples()
        self.assertEqual(is_multiples, True)
        # Test setting nval...
        yield self.bucket.set_n_val(3)
        n_val = yield self.bucket.get_n_val()
        self.assertEqual(n_val, 3)
        # Test setting multiple properties...
        yield self.bucket.set_properties({"allow_mult": False, "n_val": 2})
        is_multiples = yield self.bucket.get_allow_multiples()
        n_val = yield self.bucket.get_n_val()
        self.assertEqual(is_multiples, False)
        self.assertEqual(n_val, 2)
        log.msg('done set_bucket_properties')

