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
    def test_secondary_index(self):
        log.msg("*** secondary_index")
        yield self.bucket.enable_search()

        obj = self.bucket.new('foo1', {'field1': 'val1', 'field2': 1001})
        obj.add_index('field1_bin', 'val1')
        obj.add_index('field2_int', 1001)
        yield obj.store()

        obj = self.bucket.new('foo2', {'field1': 'val2', 'field2': 1003})
        obj.add_index('field1_bin', 'val2')
        obj.add_index('field2_int', 1003)
        yield obj.store()

        results = yield self.client.index(self.bucket_name,
                                          'field1_bin', 'val2').run()
        
        r1 = yield results[0].get()

        self.assertEqual(r1.get_key(), u'foo2')

        results = yield self.client.index(self.bucket_name, 'field2_int', 1,
                                          2000).run()

        r1 = []
        for i in results:
            r1.append((yield i.get()).get_key())

        self.assertEqual(sorted(r1),
                         ['foo1', 'foo2'])

        log.msg("done secondary_index")

