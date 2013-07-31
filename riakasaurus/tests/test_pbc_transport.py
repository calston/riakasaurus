from twisted.trial import unittest
from twisted.internet import defer

from riakasaurus import riak, transport


RIAK_CLIENT_ID = 'TEST'
BUCKET_PREFIX = 'riakasaurus.tests.'


class Tests(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.old_max_transports = transport.PBCTransport.MAX_TRANSPORTS
        transport.PBCTransport.MAX_TRANSPORTS = 3

        self.client = riak.RiakClient(client_id=RIAK_CLIENT_ID,
                port=8087, transport=transport.PBCTransport)
        self.bucket_name = BUCKET_PREFIX + self.id().rsplit('.', 1)[-1]
        self.bucket = self.client.bucket(self.bucket_name)
        yield self.bucket.purge_keys()

    @defer.inlineCallbacks
    def tearDown(self):
        transport.PBCTransport.MAX_TRANSPORTS = self.old_max_transports
        yield self.client.get_transport().quit()

    @defer.inlineCallbacks
    def test_put_raises_exception_if_max_transports_reached(self):
        data = 'My data'
        objs = [self.bucket.new_binary(str(i), data) for i in range(4)]
        ds = map(self.put_new, objs)
        res = yield defer.DeferredList(ds, consumeErrors=True)
        for success, result_or_failure in res:
            if not success and self.is_too_many_transports_failure(result_or_failure):
                return
        assert False, 'Should fail because MAX_TRANSPORTS is 3'

    @defer.inlineCallbacks
    def test_put_returns_transports_even_if_an_exception_occurs(self):
        data = 'My data'

        # Cause three TypeErrors deep in pbc_transport, hopefully not breaking the
        # transports permanently.
        for i in range(3):
            try:
                obj = self.bucket.new_binary(i, data)
                yield self.put_new(obj)
            except TypeError:
                pass

        # Previously we would get a too many transports error, but now
        # transports should have been recycled properly.
        obj = self.bucket.new_binary('my_key', data)
        yield self.put_new(obj)

    def put_new(self, obj):
        w = self.bucket.get_w(None)
        dw = self.bucket.get_dw(None)
        pw = self.bucket.get_pw(None)
        return self.client.get_transport().put_new(obj, w=w, dw=dw, pw=pw)

    def is_too_many_transports_failure(self, failure):
        return failure.value.message.startswith('too many transports')
