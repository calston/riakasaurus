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
    def test_erlang_map_reduce(self):
        """erlang map reduce"""
        log.msg('*** erlang_map_reduce')
        # Create the object...
        obj = self.bucket.new("foo", 2)
        yield obj.store()

        obj = self.bucket.new("bar", 2)
        yield obj.store()

        obj = self.bucket.new("baz", 4)
        yield obj.store()

        # Run the map...
        job = self.client \
                .add(self.bucket_name, "foo") \
                .add(self.bucket_name, "bar") \
                .add(self.bucket_name, "baz") \
                .map(["riak_kv_mapreduce", "map_object_value"]) \
                .reduce(["riak_kv_mapreduce", "reduce_set_union"])
        result = yield job.run()
        self.assertEqual(len(result), 2)
        log.msg('done erlang_map_reduce')

    @defer.inlineCallbacks
    def test_map_reduce_from_object(self):
        """map reduce from an object"""
        log.msg('*** map_reduce_from_object')
        # Create the object...
        yield self.bucket.new("foo", 2).store()
        obj = yield self.bucket.get("foo")
        job = obj.map("Riak.mapValuesJson")
        result = yield job.run()
        self.assertEqual(result, [2])
        log.msg('done map_reduce_from_object')

