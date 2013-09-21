#!/usr/bin/env python

from twisted.trial import unittest
from twisted.python import log
from twisted.internet import defer
from distutils.version import LooseVersion

from riakasaurus import riak, exceptions

RIAK_CLIENT_ID = 'TEST'
BUCKET_PREFIX = 'riakasaurus.tests.'

class RiakTest(unittest.TestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.client = riak.RiakClient(client_id=RIAK_CLIENT_ID)
        self.bucket_name = BUCKET_PREFIX + self.id().rsplit('.', 1)[-1]
        self.bucket = self.client.bucket(self.bucket_name)
        yield self.bucket.enable_search()
        yield self.bucket.purge_keys()

def buildTestCases(name):
    tests = [
        'self.client.is_alive',
        'self.client.ping',
        'self.client.stream_buckets',
        'self.client.bucket',
        'self.client.get_keys',
        'self.client.get',
        'self.client.stream_keys',
        'self.client.put', 
        'self.client.multiget', 
        'self.client.delete', 
        'self.client.mapred', 
        'self.client.solr.add',
        'self.client.solr.delete', 
        'self.client.solr.search',
        'self.client.delete_search_index', 
        'self.client.create_search_schema', 
        'self.client.get_search_schema', 
        'self.client.create_search_index', 
        'self.client.get_index',
        'self.client.index', 
    ]

    testcases = {}
    for test in tests:
        def checkMethod(self, fn=test):
            """ Runs a test lambda operation """
            f = eval(fn)

        n = test.strip('self.').replace('.', '_')
        testcases['test_%s' % n] = checkMethod

    return type(name, (RiakTest,), testcases)

Tests = buildTestCases('TestStuff')
