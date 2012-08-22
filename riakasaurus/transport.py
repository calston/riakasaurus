import urllib
from twisted.internet import defer

class HTTPTransport(object):
    
    def __init__(self, client):
        self._prefix = client._prefix
        self.host = client._host
        self.port = client._port
        self.client = client

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
        
        response = yield self.http_request('GET', url)

        headers, encoded_props = response[0:2]
        if headers['http_code'] == 200:
            props = self.client.get_encoder('application/json')(encoded_props)
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
        response = yield self.http_request('PUT', url, headers, content)

        # Handle the response...
        if (response == None):
            raise Exception('Error setting bucket properties.')

        # Check the response value...
        status = response[0]['http_code']

        if (status != 204):
            raise Exception('Error setting bucket properties.')
        defer.returnValue(response)
