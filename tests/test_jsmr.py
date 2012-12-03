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
    def test_javascript_source_map(self):
        """javascript mapping"""
        log.msg('*** javascript_source_map')
        # Create the object...
        obj = self.bucket.new("foo", 2)
        yield obj.store()
        # Run the map...
        job = self.client \
                .add(self.bucket_name, "foo") \
                .map("function (v) { return [JSON.parse(v.values[0].data)]; }")
        result = yield job.run()
        self.assertEqual(result, [2])
        log.msg('done javascript_source_map')

    @defer.inlineCallbacks
    def test_javascript_named_map(self):
        """javascript mapping with named map"""
        log.msg('*** javascript_named_map')
        # Create the object...
        obj = self.bucket.new("foo", 2)
        yield obj.store()
        # Run the map...
        job = self.client \
                .add(self.bucket_name, "foo") \
                .map("Riak.mapValuesJson")
        result = yield job.run()
        self.assertEqual(result, [2])
        log.msg('done javascript_named_map')

    @defer.inlineCallbacks
    def test_javascript_source_map_reduce(self):
        """javascript map reduce"""
        log.msg('*** javascript_source_map_reduce')
        # Create the object...
        yield self.bucket.new("foo", 2).store()
        yield self.bucket.new("bar", 3).store()
        yield self.bucket.new("baz", 4).store()
        # Run the map...
        job = self.client \
                .add(self.bucket_name, "foo") \
                .add(self.bucket_name, "bar") \
                .add(self.bucket_name, "baz") \
                .map("function (v) { return [1]; }") \
                .reduce(JAVASCRIPT_SUM)
        result = yield job.run()
        self.assertEqual(result, [3])
        log.msg('done javascript_source_map_reduce')

    @defer.inlineCallbacks
    def test_javascript_named_map_reduce(self):
        """javascript map reduce by name"""
        log.msg('*** javascript_named_map_reduce')
        # Create the object...
        yield self.bucket.new("foo", 2).store()
        yield self.bucket.new("bar", 3).store()
        yield self.bucket.new("baz", 4).store()
        # Run the map...
        job = self.client \
                .add(self.bucket_name, "foo") \
                .add(self.bucket_name, "bar") \
                .add(self.bucket_name, "baz") \
                .map("Riak.mapValuesJson") \
                .reduce("Riak.reduceSum")
        result = yield job.run()
        self.assertEqual(result, [9])
        log.msg('done javascript_named_map_reduce')

    @defer.inlineCallbacks
    def test_javascript_key_filter_map_reduce(self):
        """javascript map/reduce using key filters"""
        log.msg("javascript map reduce with key filter")
        yield self.bucket.new("foo", 2).store()
        yield self.bucket.new("bar", 3).store()
        yield self.bucket.new("baz", 4).store()
        # Run the map...
        job = self.client \
                .add({"bucket": self.bucket_name,
                      "key_filters": [["starts_with", "ba"]]}) \
                .map("function (v) { return [1]; }") \
                .reduce(JAVASCRIPT_SUM)
        result = yield job.run()
        self.assertEqual(result, [2])
        log.msg('done javascript_key_filter_map_reduce')

    def test_javascript_bucket_map_reduce(self):
        """javascript bucket map reduce"""
        log.msg('*** javascript_bucket_map_reduce')
        # Create the object...
        yield self.bucket.new("foo", 2).store()
        yield self.bucket.new("bar", 3).store()
        yield self.bucket.new("baz", 4).store()
        # Run the map...
        job = self.client \
                .add(self.bucket_name) \
                .map("Riak.mapValuesJson") \
                .reduce("Riak.reduceSum")
        result = yield job.run()
        self.assertEqual(result, [9])

    @defer.inlineCallbacks
    def test_javascript_arg_map_reduce(self):
        """javascript arguments map reduce"""
        log.msg('*** javascript_arg_map_reduce')
        # Create the object...
        obj = self.bucket.new("foo", 2)
        yield obj.store()
        # Run the map...
        job = self.client \
                .add(self.bucket_name, "foo", 5) \
                .add(self.bucket_name, "foo", 10) \
                .add(self.bucket_name, "foo", 15) \
                .add(self.bucket_name, "foo", -15) \
                .add(self.bucket_name, "foo", -5) \
                .map("function(v, arg) { return [arg]; }") \
                .reduce("Riak.reduceSum")
        result = yield job.run()
        self.assertEqual(result, [10])
        log.msg('done javascript_arg_map_reduce')


