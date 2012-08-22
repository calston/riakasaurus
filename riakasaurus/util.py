"""
.. module:: util.py

--- NOTE: All of this is pretty much deprecated by the transport.HTTPTransport object

"""

import urllib
import re
import codecs
from twisted.internet import defer
from twisted.internet import reactor
from zope.interface import implements
from twisted import version as twisted_version
from twisted.web import client
from twisted.web.http import PotentialDataLoss
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from twisted.internet.protocol import Protocol
from twisted.web._newclient import ResponseDone
from StringIO import StringIO

if twisted_version.major >= 12:
    Agent = client.Agent
else:
    class Agent(client.Agent):
        """
        client.Agent does not provide the ability to set a timeout.
        This is an Agent with a timeout.
        """

        def __init__(self, reactor,
                           contextFactory=None,
                           connectTimeout=240):
            self._reactor = reactor
            self._contextFactory = contextFactory
            self.connectTimeout = connectTimeout


        def _connect(self, scheme, host, port):
            """ connect method with a timeout. """

            cc = client.ClientCreator(self._reactor, self._protocol)
            if scheme == 'http':
                d = cc.connectTCP(host, port, timeout=self.connectTimeout)
            elif scheme == 'https':
                raise Exception('HTTPS not supported')
                #d = cc.connectSSL(host, port, self._wrapContextFactory(host, port),
                #                  timeout=self.timeout)
            else:
                d = defer.fail(SchemeNotSupported(
                        "Unsupported scheme: %r" % (scheme,)))
            return d

class StringProducer(object):
    """
    Body producer for t.w.c.Agent
    """
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        return defer.maybeDeferred(consumer.write, self.body)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class ResponseReceiver(Protocol):
    """
    Assembles HTTP response from return stream.
    """

    def __init__(self, deferred):
        self.writer = codecs.getwriter("utf_8")(StringIO())
        self.deferred = deferred

    def dataReceived(self, bytes):
        self.writer.write(bytes)

    def connectionLost(self, reason):
        if reason.check(ResponseDone) or reason.check(PotentialDataLoss):
            self.deferred.callback(self.writer.getvalue())
        else:
            self.deferred.errback(reason)

def getPageWithHeaders(contextFactory=None,
                      scheme='http', host='127.0.0.1', port=80, path='/',
                      timeout=240, *args, **kwargs):
    """Download a web page as a string.

    :returns: twisted.web.client.Agent deferred.

    Download a page. Return a deferred, which will callback with a
    page (as a string) or errback with a description of the error.

    See t.w.c.Agent to see what extra args can be passed.
    """

    def cb_recv_resp(response):
        d_resp_recvd = defer.Deferred()
        if response.length:
            response.deliverBody(ResponseReceiver(d_resp_recvd))
            return d_resp_recvd.addCallback(cb_process_resp, response)
        else:
            return cb_process_resp("", response)

    def cb_process_resp(body, response):
        _headers = {"http_code": response.code}

        for header in response.headers.getAllRawHeaders():
            _headers[header[0].lower()] = header[1][0]

        return _headers, body

    url = str("http://%s:%d%s" % (host, port, path))

    if not "headers" in kwargs:
        kwargs["headers"] = {}
    else:
        for header in kwargs["headers"]:
            kwargs["headers"][header] = [kwargs["headers"][header]]

    if not "method" in kwargs:
        kwargs["method"] == "GET"

    if "postdata" in kwargs:
        body = StringProducer(kwargs["postdata"])
    else:
        body = None

    d = Agent(reactor, connectTimeout=timeout).request(kwargs["method"],
                                      url,
                                      Headers(kwargs["headers"]),
                                      body
                                  )

    d.addCallback(cb_recv_resp)
    return d

def flatten_js(js_func):
    """Flatten a JavaScript function into a single line.

    :returns: String.

    Take a JavaScript function and strip out all of the
    newlines so that it is suitable for passing into a
    Riak map/reduce phase.
    """

    return "".join(js_func.split("\n"))

def get_value(key, array, default_value):
    """
    Overkill for array.get(key, default_value).
    Likely due to porting from another language.
    """
    if (key in array):
        return array[key]
    else:
        return default_value

def build_rest_path(client,
                    bucket=None, key=None, spec=None, params=None,
                    prefix=None):
    """
    Given a RiakClient, RiakBucket, Key, LinkSpec, and Params,
    construct and return a URL.
    """
    # Build 'http://hostname:port/prefix/bucket'
    path = ''
    path += '/' + (prefix or client._prefix)

    # Add '.../bucket'
    if bucket:
        path += '/' + urllib.quote_plus(bucket._name)

    # Add '.../key'
    if bucket and key:
        path += '/' + urllib.quote_plus(key)

    # Add query parameters.
    if (params != None):
        s = ''
        for key in params.keys():
            if (s != ''):
                s += '&'
            s += (urllib.quote_plus(key) + '=' +
                  urllib.quote_plus(str(params[key])))
        path += '?' + s

    # Return.
    return client._host, client._port, path

def http_request_deferred(method, host, port, path,
                          headers=None, obj=''):
    """
    Given a Method, URL, Headers, and Body, perform an HTTP request,
    and return deferred.
    :returns: deferred
    """
    if headers == None:
        headers = {}

    return getPageWithHeaders(contextFactory=None,
                              scheme='http',
                              host=host,
                              method=method,
                              port=port,
                              path=path,
                              headers=headers,
                              postdata=obj,
                              timeout=20)

def build_headers(headers):
    """
    Build up the header string.
    """
    headers1 = []
    for key in headers.keys():
        headers1.append('%s: %s' % (key, headers[key]))
    return headers1

def parse_http_headers(headers):
    """
    Parse an HTTP Header string into dictionary of response headers.
    """
    result = {}
    fields = headers.split("\n")
    for field in fields:
        matches = re.match("([^:]+):(.+)", field)
        if (matches == None):
            continue
        key = matches.group(1).lower()
        value = matches.group(2).strip()
        if (key in result.keys()):
            if  isinstance(result[key], list):
                result[key].append(value)
            else:
                result[key] = [result[key]].append(value)
        else:
            result[key] = value
    return result


