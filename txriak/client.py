"""
.. module:: client.py

RiakClient class

"""

import random
import base64
import urllib
import json
from twisted.internet import defer

from riakasaurus import mapreduce, util, bucket
from riakasaurus.search import RiakSearch


class RiakClient(object):
    """
    The RiakClient object holds information necessary to connect to
    Riak.
    """
    def __init__(self, host='127.0.0.1', port=8098,
                prefix='riak', mapred_prefix='mapred',
                client_id=None, r_value=2, w_value=2, dw_value=0):
        """
        Construct a new RiakClient object.

        If a client_id is not provided, generate a random one.
        """
        self._host = host
        self._port = port
        self._prefix = prefix
        self._mapred_prefix = mapred_prefix
        if client_id:
            self._client_id = client_id
        else:
            self._client_id = 'py_' + base64.b64encode(
                                      str(random.randint(1, 1073741824)))
        self._r = r_value
        self._w = w_value
        self._dw = dw_value
        self._encoders = {'application/json': json.dumps,
                          'text/json': json.dumps}
        self._decoders = {'application/json': json.loads,
                          'text/json': json.loads}
        self._solr = None

    def get_r(self):
        """
        Get the R-value setting for this RiakClient. (default 2)
        :returns: integer representing current r-value
        .. todo:: remove accessor
        """
        return self._r

    def set_r(self, r):
        """
        Set the R-value for this RiakClient. This value will be used
        for any calls to get(...) or get_binary(...) where where 1) no
        R-value is specified in the method call and 2) no R-value has
        been set in the RiakBucket.
        @param integer r - The R value.
        @return self
        .. todo:: remove accessor
        """
        self._r = r
        return self

    def get_w(self):
        """
        Get the W-value setting for this RiakClient. (default 2)
        @return integer
        .. todo:: remove accessor
        """
        return self._w

    def set_w(self, w):
        """
        Set the W-value for this RiakClient. See set_r(...) for a
        description of how these values are used.
        @param integer w - The W value.
        @return self
        .. todo:: remove accessor
        """
        self._w = w
        return self

    def get_dw(self):
        """
        Get the DW-value for this ClientOBject. (default 2)
        @return integer
        .. todo:: remove accessor
        """
        return self._dw

    def set_dw(self, dw):
        """
        Set the DW-value for this RiakClient. See set_r(...) for a
        description of how these values are used.
        @param integer dw - The DW value.
        @return self
        .. todo:: remove accessor
        """
        self._dw = dw
        return self

    def get_client_id(self):
        """
        Get the client_id for this RiakClient.
        @return string
        .. todo:: remove accessor
        """
        return self._client_id

    def set_client_id(self, client_id):
        """
        Set the client_id for this RiakClient. Should not be called
        unless you know what you are doing.
        @param string client_id - The new client_id.
        @return self
        .. todo:: remove accessor
        """
        self._client_id = client_id
        return self

    def get_encoder(self, content_type):
        """
        Get the encoding function for the provided content type.
        """
        if content_type in self._encoders:
            return self._encoders[content_type]

    def set_encoder(self, content_type, encoder):
        """
        Set the encoding function for the provided content type.

        :param encoder:
        :type encoder: function
        """
        self._encoders[content_type] = encoder
        return self

    def get_decoder(self, content_type):
        """
        Get the decoding function for the provided content type.
        """
        if content_type in self._decoders:
            return self._decoders[content_type]

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for the provided content type.

        :param decoder:
        :type decoder: function
        """
        self._decoders[content_type] = decoder
        return self

    def bucket(self, name):
        """
        Get the bucket by the specified name. Since buckets always exist,
        this will always return a RiakBucket.
        :returns: RiakBucket instance.
        """
        return bucket.RiakBucket(self, name)

    @defer.inlineCallbacks
    def is_alive(self):
        """
        Check if the Riak server for this RiakClient is alive.
        :returns: True if alive -- via deferred.
        """
        response = yield util.http_request_deferred('GET', self._host,
                                          self._port, '/ping')
        defer.returnValue((response != None) and (response[1] == 'OK'))

    def set_mapreduce(self, mreduce):
        """
        Manually set a map/reduce query.
        Useful when copying examples from the web.
        """
        mr = mapreduce.RiakMapReduce(self)
        return mr.set_mapreduce(mreduce)

    def add(self, *args):
        """
        Start assembling a Map/Reduce operation.
        see RiakMapReduce.add()
        :returns: RiakMapReduce
        """
        mr = mapreduce.RiakMapReduce(self)
        return apply(mr.add, args)

    def search(self, *args):
        """
        Start assembling a Map/Reduce operation for Riak Search
        see RiakMapReduce.search()
        """
        mr = mapreduce.RiakMapReduce(self)
        return apply(mr.search, args)

    def link(self, args):
        """
        Start assembling a Map/Reduce operation.
        see RiakMapReduce.link()
        :returns: RiakMapReduce
        """
        mr = mapreduce.RiakMapReduce(self)
        return apply(mr.link, args)

    @defer.inlineCallbacks
    def list_buckets(self):
        """
        Retrieve a list of all buckets.

        :returns: list -- via deferred
        """
        # Create the request
        params = {"buckets": "true"}

        host, port, url = util.build_rest_path(self,
                                                    None,
                                                    None,
                                                    None,
                                                    params)
        raw_response = yield util.http_request_deferred('GET', host,
                                                             port, url)

        if raw_response[0]["http_code"] != 200:
            raise Exception('Error listing buckets.')

        # Parse the bucket list
        buckets = json.loads(raw_response[1])["buckets"]

        defer.returnValue([urllib.unquote(x) for x in buckets])

    def index(self, *args):
        """
        Start assembling a Map/Reduce operation based on secondary
        index query results.

        :rtype: :class:`RiakMapReduce`
        """
        mr = mapreduce.RiakMapReduce(self)
        return apply(mr.index, args)

    def map(self, *args):
        """
        Start assembling a Map/Reduce operation.
        see RiakMapReduce.map()
        :returns: RiakMapReduce
        """
        mr = mapreduce.RiakMapReduce(self)
        return apply(mr.map, args)

    def reduce(self, *args):
        """
        Start assembling a Map/Reduce operation.
        see RiakMapReduce.reduce()
        :returns: RiakMapReduce
        """
        mr = mapreduce.RiakMapReduce(self)
        return apply(mr.reduce, args)

    def solr(self):
        if self._solr is None:
            self._solr = RiakSearch(self, host=self._host, port=self._port)

        return self._solr


if __name__ == "__main__":
    pass
