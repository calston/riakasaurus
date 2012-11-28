#!/usr/bin/env python
"""
riakasaurus_pbc trial
"""

from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.python import log
from tx_riak_pb import RiakPBCClient
from riak_kv_pb2 import *
from riak_pb2 import *

VERBOSE = False
# uncomment to activate logging
if VERBOSE:
    import sys
    log.startLogging(sys.stderr)


class Tests(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.client = yield RiakPBCClient().connect('127.0.0.1', 8087)
        self.client.debug = 0

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.client.quit()

    @defer.inlineCallbacks
    def test_ping(self):
        log.msg("*** testing ping")
        res = yield self.client.ping()
        self.assertTrue(res)
        log.msg("done testing ping")

    @defer.inlineCallbacks
    def test_setgetClientId(self):
        log.msg("*** testing getClientId")
        yield self.client.setClientId('MyClientId')
        res = yield self.client.getClientId()
        self.assertEqual(res.client_id, 'MyClientId')
        log.msg("done testing getClientId")

    @defer.inlineCallbacks
    def test_getServerInfo(self):
        log.msg("*** testing getServerInfo")
        info = yield self.client.getServerInfo()
        self.assertTrue(isinstance(info, RpbGetServerInfoResp))
        log.msg("done testing getServerInfo")
        
    @defer.inlineCallbacks
    def test_put(self):
        log.msg("*** testing put")
        put = yield self.client.put('bucket','key', 'foo')
        self.assertTrue(isinstance(put, RpbPutResp))
       
        log.msg("done testing put")

    @defer.inlineCallbacks
    def test_get(self):
        log.msg("*** testing get")
        put = yield self.client.put('bucket','key', 'foo')
        self.assertTrue(isinstance(put, RpbPutResp))
       
        result = yield self.client.get('bucket','key')
        self.assertTrue(isinstance(result, RpbGetResp))
        self.assertEqual(result.content[0].value,'foo')
       
        log.msg("done testing get")

    @defer.inlineCallbacks
    def test_update(self):
        log.msg("*** testing update")
        # make sure "foo" is in
        put = yield self.client.put('bucket','key', 'foo')
        self.assertTrue(isinstance(put, RpbPutResp))

        # retrieve it
        result = yield self.client.get('bucket','key')
        self.assertTrue(isinstance(result, RpbGetResp))
        self.assertEqual(result.content[0].value,'foo')

        result2 = yield self.client.put('bucket','key','bla',result.vclock)
        self.assertTrue(isinstance(result2, RpbPutResp))

        # retrieve updated
        result = yield self.client.get('bucket','key')
        self.assertTrue(isinstance(result, RpbGetResp))
        self.assertEqual(result.content[0].value,'bla')
        
        log.msg("done testing update")


    @defer.inlineCallbacks
    def test_links(self):
        log.msg("*** testing links")
        # make sure "foo" is in
        c = {'value' : 'foo',
             'content_type' : 'text/text',
             'content_encoding' : 'UTF8',
             'links' : [
                ('bucket', 'subkey', 'tag'),
                ('bucket', 'nochnkey', ''),
                ]
             }
        put = yield self.client.put('bucket','key', c)
        self.assertTrue(isinstance(put, RpbPutResp))

        # retrieve it
        result = yield self.client.get('bucket','key')
        self.assertTrue(isinstance(result, RpbGetResp))
        self.assertEqual(result.content[0].value,'foo')
        self.assertEqual(result.content[0].content_type,'text/text')
        self.assertEqual(result.content[0].content_encoding,'UTF8')
        self.assertTrue(len(result.content[0].links) == 2)
        log.msg("done testing update")
    
