from zope.interface import Interface

from twisted.internet import defer

from distutils.version import StrictVersion

versions = {
    1: StrictVersion("1.0.0"),
    1.1: StrictVersion("1.1.0"),
    1.2: StrictVersion("1.2.0")
    }


class ITransport(Interface):
    def get_keys(self, bucket):
        """
        list keys for a given bucket
        """

    def put(self, robj, w = None, dw = None, pw = None, return_body = True, if_none_match=False):
        """
        store a riak_object
        """

    def put_new(self, robj, w=None, dw=None, pw=None, return_body=True, if_none_match=False):
        """
        store a riak_object and generate a key for it
        """

    def get(self, robj, r = None, pr = None, vtag = None):
        """
        fetch a key from the server
        """

    def delete(self, robj, rw=None, r = None, w = None, dw = None, pr = None, pw = None):
        """
        delete a key from the bucket
        """

    def server_version(self):
        """
        return cached server version
        """

    def _server_version(self):
        """
        Gets the server version from the server. To be implemented by
        the individual transport class.
        :rtype string
        """
    def get_buckets(self):
        """
        return the existing buckets
        """

    def ping(self):
        """
        Check server is alive
        """

    def set_bucket_props(self, bucket, props):
        """
        Set bucket properties
        """

    def get_bucket_props(self, bucket):
        """
        get bucket properties
        """


class FeatureDetection(object):
    _s_version = None

    def _server_version(self):
        """
        Gets the server version from the server. To be implemented by
        the individual transport class.
        :rtype string
        """
        raise NotImplementedError

    @defer.inlineCallbacks
    def phaseless_mapred(self):
        """
        Whether MapReduce requests can be submitted without phases.
        :rtype bool
        """
        d = yield self.server_version()
        defer.returnValue(d >= versions[1.1])

    @defer.inlineCallbacks
    def pb_indexes(self):
        """
        Whether secondary index queries are supported over Protocol
        Buffers

        :rtype bool
        """
        d = yield self.server_version()
        defer.returnValue(d >= versions[1.2])

    @defer.inlineCallbacks
    def pb_search(self):
        """
        Whether search queries are supported over Protocol Buffers
        :rtype bool
        """
        d = yield self.server_version()
        defer.returnValue(d >= versions[1.2])

    @defer.inlineCallbacks
    def pb_conditionals(self):
        """
        Whether conditional fetch/store semantics are supported over
        Protocol Buffers
        :rtype bool
        """
        d = yield self.server_version()
        defer.returnValue(d >= versions[1])

    @defer.inlineCallbacks
    def quorum_controls(self):
        """
        Whether additional quorums and FSM controls are available,
        e.g. primary quorums, basic_quorum, notfound_ok
        :rtype bool
        """
        d = yield self.server_version()
        defer.returnValue(d >= versions[1])

    @defer.inlineCallbacks
    def tombstone_vclocks(self):
        """
        Whether 'not found' responses might include vclocks
        :rtype bool
        """
        d = yield self.server_version()
        defer.returnValue(d >= versions[1])

    @defer.inlineCallbacks
    def pb_head(self):
        """
        Whether partial-fetches (vclock and metadata only) are
        supported over Protocol Buffers
        :rtype bool
        """
        d = yield self.server_version()
        defer.returnValue(d >= versions[1])

    @defer.inlineCallbacks
    def server_version(self):
        if not self._s_version:
            self._s_version = yield self._server_version()

        defer.returnValue(StrictVersion(self._s_version))

