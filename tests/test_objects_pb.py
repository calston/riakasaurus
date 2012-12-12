from twisted.internet import defer, reactor
from twisted.python import log

from riakasaurus.tx_riak_pb import RiakPBCClient
from riakasaurus.riak_kv_pb2 import *
from riakasaurus.riak_pb2 import *

from test_objects import Tests
from riakasaurus import riak, transport

RIAK_CLIENT_ID = 'TEST'
BUCKET_PREFIX = 'riakasaurus.tests.'

VERBOSE = False
# uncomment to activate logging
if VERBOSE:
    import sys
    log.startLogging(sys.stderr)


class Tests_PB(Tests):

    @defer.inlineCallbacks
    def setUp(self):
        self.client = riak.RiakClient(client_id=RIAK_CLIENT_ID,
                                      host='127.0.0.1',
                                      port=8087,
                                      transport = transport.PBCTransport)
        # self.client.debug = 0
        # self.client.get_transport().debug = 0
        self.bucket_name = BUCKET_PREFIX + self.id().rsplit('.', 1)[-1]
        self.bucket = self.client.bucket(self.bucket_name)
        yield self.bucket.purge_keys()

    @defer.inlineCallbacks
    def tearDown(self):
        # shut down pb connection explicitly
        yield self.client.get_transport().quit()

    @defer.inlineCallbacks
    def test_meta_data_simple(self):
        """
        differs from http twisted in that key case is
        maintained by PB, but not via HTTP
        """
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

        # differs from http, PB maintains case
        self.assertTrue(meta_key1 in metas)
        self.assertTrue(metas[meta_key1] == meta_data)
        log.msg('done meta_data_simple')
        
    def test_link_walking(self):
        """mapred not supported yet, so skip test"""
        self.assertTrue(True)
        

        
