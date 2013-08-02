#!/usr/bin/env python
"""
tests for RiakPBCClient, trial
"""

from twisted.trial import unittest
from twisted.internet import defer
from twisted.python import log
from twisted.internet.error import ConnectionRefusedError

from riakasaurus.transport import pbc

from riakasaurus import riak, transport

VERBOSE = False
# uncomment to activate logging
if VERBOSE:
    import sys
    log.startLogging(sys.stderr)


class Test_PBCClient(unittest.TestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.client = yield pbc.RiakPBCClient().connect('127.0.0.1', 8087)
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
        self.assertTrue(isinstance(info, pbc.RpbGetServerInfoResp))
        log.msg("done testing getServerInfo")

    @defer.inlineCallbacks
    def test_put(self):
        log.msg("*** testing put")
        put = yield self.client.put('bucket', 'key', 'foo')
        self.assertTrue(isinstance(put, pbc.RpbPutResp))

        log.msg("done testing put")

    @defer.inlineCallbacks
    def test_get(self):
        log.msg("*** testing get")
        put = yield self.client.put('bucket', 'key', 'foo')
        self.assertTrue(isinstance(put, pbc.RpbPutResp))

        result = yield self.client.get('bucket', 'key')
        self.assertTrue(isinstance(result, pbc.RpbGetResp))
        self.assertEqual(result.content[0].value, 'foo')

        log.msg("done testing get")

    @defer.inlineCallbacks
    def test_update(self):
        log.msg("*** testing update")
        # make sure "foo" is in
        put = yield self.client.put('bucket', 'key', 'foo')
        self.assertTrue(isinstance(put, pbc.RpbPutResp))

        # retrieve it
        result = yield self.client.get('bucket', 'key')
        self.assertTrue(isinstance(result, pbc.RpbGetResp))
        self.assertEqual(result.content[0].value, 'foo')

        result2 = yield self.client.put('bucket', 'key', 'bla', result.vclock)
        self.assertTrue(isinstance(result2, pbc.RpbPutResp))

        # retrieve updated
        result = yield self.client.get('bucket', 'key')
        self.assertTrue(isinstance(result, pbc.RpbGetResp))
        self.assertEqual(result.content[0].value, 'bla')

        log.msg("done testing update")

    @defer.inlineCallbacks
    def test_delete(self):
        log.msg("*** testing delete")

        # make sure "foo" is in
        put = yield self.client.put('bucket', 'delete_key', 'foo')
        self.assertTrue(isinstance(put, pbc.RpbPutResp))

        # remove it
        result = yield self.client.delete('bucket', 'delete_key')
        self.assertEqual(result, True)

        # try to fetch it
        result = yield self.client.get('bucket', 'delete_key')
        self.assertTrue(isinstance(result, pbc.RpbGetResp))
        self.assertEqual(len(result.content), 0)
        log.msg("done testing delete")

    @defer.inlineCallbacks
    def test_getKeys(self):
        log.msg("*** testing getKeys")

        # get existing keys
        res = yield self.client.getKeys('bucket')
        self.assertTrue(isinstance(res, list))

        log.msg("done testing getKeys")

    @defer.inlineCallbacks
    def test_getBuckets(self):
        log.msg("*** testing getBuckets")

        # make sure "foo" is in
        put = yield self.client.put('bucket', 'key', 'foo')
        self.assertTrue(isinstance(put, pbc.RpbPutResp))

        # get existing buckets message
        res = yield self.client.getBuckets()
        buckets = [b for b in res.buckets]
        self.assertTrue('bucket' in buckets)
        log.msg("done testing getBuckets")

    @defer.inlineCallbacks
    def test_setGetBucketProperties(self):
        log.msg("*** testing setGetBucketProperties")

        # make sure "foo" is in
        put = yield self.client.put('anotherbucket', 'key', 'foo')
        self.assertTrue(isinstance(put, pbc.RpbPutResp))

        # get existing buckets message
        res = yield self.client.setBucketProperties('anotherbucket', n_val=2,
            allow_mult=True)
        self.assertTrue(res == True)

        res = yield self.client.getBucketProperties('anotherbucket')
        self.assertEqual(res.props.n_val, 2)

        yield self.client.delete('anotherbucket', 'key')

        log.msg("done testing setGetBucketProperties")

    @defer.inlineCallbacks
    def test_links(self):
        log.msg("*** testing links")

        # retrieve it
        head = yield self.client.get('bucket', 'key', head=True)

        # make sure "foo" is in
        content = {
            'value': 'foo',
            'content_type': 'text/text',
            'content_encoding': 'UTF8',
            'links': [
                ('bucket', 'subkey', 'tag'),
                ('bucket', 'nochnkey', ''),
            ]
        }
        put = yield self.client.put('bucket', 'key', content,
            vclock=head.vclock)

        self.assertTrue(isinstance(put, pbc.RpbPutResp))

        # retrieve it
        result = yield self.client.get('bucket', 'key')
        self.assertTrue(isinstance(result, pbc.RpbGetResp))

        self.assertEqual(result.content[0].value, 'foo')
        self.assertEqual(result.content[0].content_type, 'text/text')
        self.assertEqual(result.content[0].content_encoding, 'UTF8')
        self.assertTrue(len(result.content[0].links) == 2)

        log.msg("done testing links")

    @defer.inlineCallbacks
    def test_connection_refused(self):
        try:
            # Connection to a host that does not exist.
            yield pbc.RiakPBCClient().connect('127.0.0.17', 8087)
            assert False, 'ConnectionRefusedError not raised as expected.'
        except ConnectionRefusedError:
            pass
