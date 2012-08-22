from twisted.internet import defer, reactor, protocol
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

import urllib
import StringIO


class BodyReceiver(protocol.Protocol):
    """ Simple buffering consumer for body objects """
    def __init__(self, finished):
        self.finished = finished 
        self.buffer = StringIO.StringIO()

    def dataReceived(self, buffer):
        self.buffer.write(buffer)

    def connectionLost(self, reason):
        self.buffer.seek(0)
        self.finished.callback(self.buffer)

class HTTPTransport(object):
    """ HTTP Transport for Riak """
    def __init__(self, client):
        self._prefix = client._prefix
        self.host = client._host
        self.port = client._port
        self.client = client

    def http_response(self, response):
        def haveBody(body):

            headers = {"http_code": response.code}
            for key, val in response.headers.getAllRawHeaders():
                headers[key.lower()] = val[0]
            
            return headers, body.read()

        if response.length:
            d = defer.Deferred()
            response.deliverBody(BodyReceiver(d))
            return d.addCallback(haveBody)
        else:
            return haveBody(StringIO.StringIO(""))

    def http_request(self, method, path, headers=None, body=None):
        url = "http://%s:%s%s" % (self.host, self.port, path)

        if headers:
            headers = Headers(headers)

        return Agent(reactor).request(
                method, url, headers, body
            ).addCallback(self.http_response)

    def build_rest_path(self, bucket=None, key=None, params=None, prefix=None) :
        """
        Given a RiakClient, RiakBucket, Key, LinkSpec, and Params,
        construct and return a URL.
        """
        # Build 'http://hostname:port/prefix/bucket'
        path = ''
        path += '/' + (prefix or self._prefix)

        # Add '.../bucket'
        if bucket is not None:
            path += '/' + urllib.quote_plus(bucket._name)

        # Add '.../key'
        if key is not None:
            path += '/' + urllib.quote_plus(key)

        # Add query parameters.
        if params is not None:
            s = ''
            for key in params.keys():
                if params[key] is not None:
                    if s != '': s += '&'
                    s += urllib.quote_plus(key) + '=' + urllib.quote_plus(str(params[key]))
            path += '?' + s

        # Return.
        return path

    @defer.inlineCallbacks
    def get_keys(self, bucket):
        params = {'props' : 'True', 'keys' : 'true'}
        url = self.build_rest_path(bucket, params=params)
        

        headers, encoded_props = yield self.http_request('GET', url)

        if headers['http_code'] == 200:
            props = self.client.get_decoder('application/json')(encoded_props)
        else:
            raise Exception('Error getting bucket properties.') 

        defer.returnValue(props['keys'])

    @defer.inlineCallbacks
    def set_bucket_props(self, bucket, props):
        """
        Set bucket properties
        returns a deferred
        """ 

        host, port, url = util.build_rest_path(bucket)
        headers = {'Content-Type': 'application/json'}
        content = json.dumps({'props': props})

        #Run the request...
        headers, response = yield self.http_request('PUT', url, headers, content)

        # Handle the response...
        if (response == None):
            raise Exception('Error setting bucket properties.')

        # Check the response value...
        status = headers['http_code']

        if (status != 204):
            raise Exception('Error setting bucket properties.')
        defer.returnValue(response)
