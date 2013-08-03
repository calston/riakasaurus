from zope.interface import implements

from twisted.protocols.basic import LineReceiver
LineReceiver.MAX_LENGTH = 1024 * 1024 * 64

from twisted.internet import defer, reactor, protocol
from twisted.python import log
import logging

from distutils.version import StrictVersion

import time

# MD_ resources
from riakasaurus.metadata import *

from riakasaurus.riak_index_entry import RiakIndexEntry
from riakasaurus.mapreduce import RiakLink
from riakasaurus import exceptions

# protobuf
from riakasaurus.transport import transport, pbc

LOGLEVEL_DEBUG = 1
LOGLEVEL_TRANSPORT = 2
LOGLEVEL_TRANSPORT_VERBOSE = 4


class StatefulTransport(object):
    def __init__(self):
        self.__transport = None
        self.__state = 'idle'
        self.__created = time.time()
        self.__used = time.time()

    def __repr__(self):
        return '<StatefulTransport idle=%.2fs state=\'%s\' transport=%s>' % (
            time.time() - self.__used, self.__state, self.__transport)

    def __enter__(self):
        return self.getTransport()

    def __exit__(self, exc_type, exc_value, traceback):
        self.setIdle()

    def isActive(self):
        return self.__state == 'active'

    def setActive(self):
        self.__state = 'active'
        self.__used = time.time()

    def isIdle(self):
        return self.__state == 'idle'

    def setIdle(self):
        self.__state = 'idle'
        self.__used = time.time()

    def setTransport(self, transport):
        self.__transport = transport

    def getTransport(self):
        return self.__transport

    def age(self):
        return time.time() - self.__used

    def isDisconnected(self):
        transport = self.getTransport()
        return transport and transport.isDisconnected()


class PBCTransport(transport.FeatureDetection):
    """ Protocoll buffer transport for Riak """

    implements(transport.ITransport)

    debug = 0
    logToLevel = logging.INFO
    MAX_TRANSPORTS = 50
    MAX_IDLETIME = 5 * 60     # in seconds
    # how often (in seconds) the garbage collection should run
    # XXX Why the hell do we even have to override GC?
    GC_TIME = 120

    def __init__(self, client):
        self._prefix = client._prefix
        self.host = client._host
        self.port = client._port
        self.client = client
        self._client_id = None
        self._transports = []    # list of transports, empty on start
        self._gc = reactor.callLater(self.GC_TIME, self._garbageCollect)
        self.timeout = client.request_timeout

    def setTimeout(self, t):
        self.timeout = t

    @defer.inlineCallbacks
    def _getFreeTransport(self):
        foundOne = False

        # Discard disconnected transports.
        self._transports = [x for x in self._transports if not x.isDisconnected()]

        for stp in self._transports:
            if stp.isIdle():
                stp.setActive()
                stp.getTransport().setTimeout(self.timeout)
                foundOne = True
                if self.debug & LOGLEVEL_TRANSPORT_VERBOSE:
                    log.msg("[%s] aquired idle transport[%d]: %s" % (
                            self.__class__.__name__,
                            len(self._transports), stp
                        ), logLevel=self.logToLevel)
                defer.returnValue(stp)
        if not foundOne:
            if len(self._transports) >= self.MAX_TRANSPORTS:
                raise Exception("too many transports, aborting")

            # nothin free, create a new protocol instance, append
            # it to self._transports and return it

            # insert a placeholder into self._transports to avoid race
            # conditions with the MAX_TRANSPORTS check above.
            stp = StatefulTransport()
            stp.setActive()
            idx = len(self._transports)
            self._transports.append(stp)

            # create the transport and use it to configure the placeholder.
            try:
                transport = yield pbc.RiakPBCClient().connect(self.host, self.port)
                stp.setTransport(transport)
                if self.timeout:
                    transport.setTimeout(self.timeout)
                if self.debug & LOGLEVEL_TRANSPORT:
                    log.msg("[%s] allocate new transport[%d]: %s" % (
                            self.__class__.__name__, idx, stp
                        ), logLevel=self.logToLevel)
                defer.returnValue(stp)
            except Exception:
                self._transports.remove(stp)
                raise


    @defer.inlineCallbacks
    def _garbageCollect(self):
        self._gc = reactor.callLater(self.GC_TIME, self._garbageCollect)
        for idx, stp in enumerate(self._transports):
            if (stp.isIdle() and stp.age() > self.MAX_IDLETIME):
                yield stp.getTransport().quit()
                if self.debug & LOGLEVEL_TRANSPORT:
                    log.msg("[%s] expire idle transport[%d] %s" % (
                            self.__class__.__name__,
                            idx,
                            stp
                        ), logLevel=self.logToLevel)

                    log.msg("[%s] %s" % (
                            self.__class__.__name__,
                            self._transports
                        ), logLevel=self.logToLevel)

                self._transports.remove(stp)
            elif self.timeout and stp.isActive() and stp.age() > self.timeout:
                yield stp.getTransport().quit()
                if self.debug & LOGLEVEL_TRANSPORT:
                    log.msg("[%s] expire timeouted transport[%d] %s" % (
                            self.__class__.__name__,
                            idx,
                            stp
                        ), logLevel=self.logToLevel)

                    log.msg("[%s] %s" % (
                            self.__class__.__name__,
                            self._transports
                        ), logLevel=self.logToLevel)

                self._transports.remove(stp)

    @defer.inlineCallbacks
    def quit(self):
        self._gc.cancel()      # cancel the garbage collector

        for stp in self._transports:
            if self.debug & LOGLEVEL_DEBUG:
                log.msg("[%s] transport[%d].quit() %s" % (
                        self.__class__.__name__,
                        len(self._transports),
                        stp
                    ), logLevel=self.logToLevel)

            yield (stp.getTransport() and stp.getTransport().quit())

    def __del__(self):
        """on shutdown, close all transports"""
        self.quit()

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False):

        ret = self.__put(robj, w, dw, pw,
                         return_body=return_body, if_none_match=if_none_match)

        if return_body:
            return ret
        else:
            return None

    def put_new(self, robj, w=None, dw=None, pw=None, return_body=True,
                if_none_match=False):
        ret = self.__put(robj, w, dw, pw,
                         return_body=return_body, if_none_match=if_none_match)
        if return_body:
            return ret
        else:
            return (ret[0], None, None)

    @defer.inlineCallbacks
    def __put(self, robj, w=None, dw=None, pw=None, return_body=True,
              if_none_match=False):
        # std kwargs
        kwargs = {
                'w': w,
                'dw': dw,
                'pw': pw,
                'return_body': return_body,
                'if_none_match': if_none_match
            }
        # vclock
        vclock = robj.vclock() or None

        payload = {
                'value': robj.get_encoded_data(),
                'content_type': robj.get_content_type(),
            }

        # links
        links = robj.get_links()
        if links:
            payload['links'] = []
            for l in links:
                payload['links'].append(
                    (l.get_bucket(), l.get_key(), l.get_tag())
                )

        # usermeta
        if robj.get_usermeta():
            payload['usermeta'] = []
            for key, value in robj.get_usermeta().iteritems():
                payload['usermeta'].append((key, value))

        # indexes
        if robj.get_indexes():
            payload['indexes'] = []
            for index in robj.get_indexes():
                payload['indexes'].append(
                    (index.get_field(), index.get_value())
                )

        # aquire transport, fire, release
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.put(robj.get_bucket().get_name(),
                                      robj.get_key(),
                                      payload,
                                      vclock,
                                      **kwargs
                                      )
        defer.returnValue(self.parseRpbGetResp(ret))

    @defer.inlineCallbacks
    def get(self, robj, r=None, pr=None, vtag=None):

        # ***FIXME*** whats vtag for? ignored for now

        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.get(robj.get_bucket().get_name(),
                                      robj.get_key(),
                                      r=r,
                                      pr=pr)

        defer.returnValue(self.parseRpbGetResp(ret))

    @defer.inlineCallbacks
    def head(self, robj, r=None, pr=None, vtag=None):
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.get(robj.get_bucket().get_name(),
                                      robj.get_key(),
                                      r=r,
                                      pr=pr,
                                      head=True)

        defer.returnValue(self.parseRpbGetResp(ret))

    @defer.inlineCallbacks
    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None):
        """
        Delete an object.
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        kwargs = {'rw': rw, 'r': r, 'w': w, 'dw': dw, 'pr': pr, 'pw': pw}
        headers = {}

        ts = yield self.tombstone_vclocks()
        if ts and robj.vclock() is not None:
            kwargs['vclock'] = robj.vclock()

        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.delete(robj.get_bucket().get_name(),
                                         robj.get_key(),
                                         **kwargs
                                         )

        defer.returnValue(ret)

    @defer.inlineCallbacks
    def get_buckets(self):
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.getBuckets()
        defer.returnValue([x for x in ret.buckets])

    @defer.inlineCallbacks
    def server_version(self):
        if not self._s_version:
            self._s_version = yield self._server_version()

        defer.returnValue(StrictVersion(self._s_version))

    @defer.inlineCallbacks
    def _server_version(self):
        with (yield self._getFreeTransport()) as transport:
            stats = yield transport.getServerInfo()

        if stats is not None:
            if self.debug % LOGLEVEL_DEBUG:
                log.msg("[%s] fetched server version: %s" % (
                    self.__class__.__name__,
                    stats.server_version
                ), logLevel=self.logToLevel)
            defer.returnValue(stats.server_version)
        else:
            defer.returnValue("0.14.0")

    @defer.inlineCallbacks
    def ping(self):
        """
        Check server is alive
        """
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.ping()
        defer.returnValue(ret == True)

    @defer.inlineCallbacks
    def set_bucket_props(self, bucket, props):
        """
        Set bucket properties
        """
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.setBucketProperties(bucket.get_name(), **props)
        defer.returnValue(ret == True)

    @defer.inlineCallbacks
    def get_bucket_props(self, bucket):
        """
        get bucket properties
        """
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.getBucketProperties(bucket.get_name())
        defer.returnValue({
            'n_val': ret.props.n_val,
            'allow_mult': ret.props.allow_mult
        })

    @defer.inlineCallbacks
    def get_keys(self, bucket):
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.getKeys(bucket.get_name())
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def get_index(self, bucket, index, startkey, endkey=None):
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.get_index(bucket, index, startkey, endkey=endkey)
        defer.returnValue(ret)

    def parseRpbGetResp(self, res):
        """
        adaptor for a RpbGetResp message
        message RpbGetResp {
           repeated RpbContent content = 1;
           optional bytes vclock = 2;
           optional bool unchanged = 3;
        }
        """
        if res == True:         # empty response
            return None
        vclock = res.vclock
        resList = []
        for content in res.content:
            # iterate over RpbContent field
            metadata = {MD_USERMETA: {}, MD_INDEX: []}
            data = content.value
            if content.HasField('content_type'):
                metadata[MD_CTYPE] = content.content_type

            if content.HasField('charset'):
                metadata[MD_CHARSET] = content.charset

            if content.HasField('content_encoding'):
                metadata[MD_ENCODING] = content.content_encoding

            if content.HasField('vtag'):
                metadata[MD_VTAG] = content.vtag

            if content.HasField('last_mod'):
                metadata[MD_LASTMOD] = content.last_mod

            if content.HasField('deleted'):
                metadata[MD_DELETED] = content.deleted

            if len(content.links):
                metadata[MD_LINKS] = []
                for l in content.links:
                    metadata[MD_LINKS].append(
                        RiakLink(l.bucket, l.key, l.tag)
                    )

            if len(content.usermeta):
                metadata[MD_USERMETA] = {}
                for md in content.usermeta:
                    metadata[MD_USERMETA][md.key] = md.value

            if len(content.indexes):
                metadata[MD_INDEX] = []
                for ie in content.indexes:
                    metadata[MD_INDEX].append(RiakIndexEntry(ie.key, ie.value))
            resList.append((metadata, data))
        return vclock, resList

    def decodeJson(self, s):
        return self.client.get_decoder('application/json')(s)

    def encodeJson(self, s):
        return self.client.get_encoder('application/json')(s)

    # def deferred_sleep(self,secs):
    #     """
    #     fake deferred sleep

    #     @param secs: time to sleep
    #     @type secs: float
    #     """
    #     d = defer.Deferred()
    #     reactor.callLater(secs, d.callback, None)
    #     return d
