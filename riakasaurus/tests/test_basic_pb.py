from twisted.internet import defer, reactor
from twisted.python import log
from twisted.trial import unittest

from test_basic import BasicTestsMixin
from riakasaurus import riak, transport

RIAK_CLIENT_ID = 'TEST'
BUCKET_PREFIX = 'riakasaurus.tests.'

VERBOSE = False
# uncomment to activate logging
if VERBOSE:
    import sys
    log.startLogging(sys.stderr)


class Tests_PB(unittest.TestCase, BasicTestsMixin):

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
    def test_is_alive(self):
        alive = yield self.client.is_alive()
        self.assertEqual(alive, True)
