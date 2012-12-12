from twisted.protocols.basic import Int32StringReceiver
from twisted.internet.protocol import ClientFactory
from twisted.internet.defer import Deferred
from twisted.internet import defer, reactor
from twisted.python.failure import Failure

from struct import pack, unpack

from pprint import pformat

# generated code from *.proto message definitions
from riak_kv_pb2 import *
from riak_pb2 import *

## Protocol codes
MSG_CODE_ERROR_RESP = 0
MSG_CODE_PING_REQ = 1
MSG_CODE_PING_RESP = 2
MSG_CODE_GET_CLIENT_ID_REQ = 3
MSG_CODE_GET_CLIENT_ID_RESP = 4
MSG_CODE_SET_CLIENT_ID_REQ = 5
MSG_CODE_SET_CLIENT_ID_RESP = 6
MSG_CODE_GET_SERVER_INFO_REQ = 7
MSG_CODE_GET_SERVER_INFO_RESP = 8
MSG_CODE_GET_REQ = 9
MSG_CODE_GET_RESP = 10
MSG_CODE_PUT_REQ = 11
MSG_CODE_PUT_RESP = 12
MSG_CODE_DEL_REQ = 13
MSG_CODE_DEL_RESP = 14
MSG_CODE_LIST_BUCKETS_REQ = 15
MSG_CODE_LIST_BUCKETS_RESP = 16
MSG_CODE_LIST_KEYS_REQ = 17
MSG_CODE_LIST_KEYS_RESP = 18
MSG_CODE_GET_BUCKET_REQ = 19
MSG_CODE_GET_BUCKET_RESP = 20
MSG_CODE_SET_BUCKET_REQ = 21
MSG_CODE_SET_BUCKET_RESP = 22
MSG_CODE_MAPRED_REQ = 23
MSG_CODE_MAPRED_RESP = 24
MSG_CODE_INDEX_REQ = 25
MSG_CODE_INDEX_RESP = 26
MSG_CODE_SEARCH_QUERY_REQ = 27
MSG_CODE_SEARCH_QUERY_RESP = 28



class RiakPBCException(Exception):
    pass

def toHex(s):
    lst = []
    for ch in s:
        hv = hex(ord(ch)).replace('0x', '')
        if len(hv) == 1:
            hv = '0'+hv
        lst.append(hv+ ' ')

    return reduce(lambda x,y:x+y, lst)

class RiakPBC(Int32StringReceiver):

    MAX_LENGTH = 9999999

    riakResponses = {
        MSG_CODE_ERROR_RESP           : RpbErrorResp,
        MSG_CODE_GET_CLIENT_ID_RESP   : RpbGetClientIdResp,
        MSG_CODE_GET_SERVER_INFO_RESP : RpbGetServerInfoResp,
        MSG_CODE_GET_RESP             : RpbGetResp,
        MSG_CODE_PUT_RESP             : RpbPutResp,
        MSG_CODE_LIST_KEYS_RESP       : RpbListKeysResp,
        MSG_CODE_LIST_BUCKETS_RESP    : RpbListBucketsResp,
        MSG_CODE_GET_BUCKET_RESP      : RpbGetBucketResp,
        MSG_CODE_GET_SERVER_INFO_RESP : RpbGetServerInfoResp,
        }

    PBMessageTypes = {
        0 : 'ERROR_RESP',
        1 : 'PING_REQ',
        2 : 'PING_RESP',
        3 : 'GET_CLIENT_ID_REQ',
        4 : 'GET_CLIENT_ID_RESP',
        5 : 'SET_CLIENT_ID_REQ',
        6 : 'SET_CLIENT_ID_RESP',
        7 : 'GET_SERVER_INFO_REQ',
        8 : 'GET_SERVER_INFO_RESP',
        9 : 'GET_REQ',
        10 : 'GET_RESP',
        11 : 'PUT_REQ',
        12 : 'PUT_RESP',
        13 : 'DEL_REQ',
        14 : 'DEL_RESP',
        15 : 'LIST_BUCKETS_REQ',
        16 : 'LIST_BUCKETS_RESP',
        17 : 'LIST_KEYS_REQ',
        18 : 'LIST_KEYS_RESP',
        19 : 'GET_BUCKET_REQ',
        20 : 'GET_BUCKET_RESP',
        21 : 'SET_BUCKET_REQ',
        22 : 'SET_BUCKET_RESP',
        23 : 'MAPRED_REQ',
        24 : 'MAPRED_RESP',
        25 : 'INDEX_REQ',
        26 : 'INDEX_RESP',
        27 : 'SEARCH_QUERY_REQ',
        28 : 'SEARCH_QUERY_RESP',
        }
    

    nonMessages = (MSG_CODE_PING_RESP,
                   MSG_CODE_DEL_RESP,
                   MSG_CODE_SET_BUCKET_RESP,
                   MSG_CODE_SET_CLIENT_ID_RESP)

    rwNums = {
        'one'     : 4294967295-1,
        'quorum'  : 4294967295-2,
        'all'     : 4294967295-3,
        'default' : 4294967295-4,
        }

    timeout = None
    timeoutd = None
    debug = 0

    # ------------------------------------------------------------------
    # Server Operations .. setClientId, getClientId, getServerInfo, ping
    # ------------------------------------------------------------------
    def setClientId(self,clientId):
        code = pack('B',MSG_CODE_SET_CLIENT_ID_REQ)
        request = RpbSetClientIdReq()
        request.client_id = clientId
        return self.__send(code,request)

    def getClientId(self):
        code = pack('B',MSG_CODE_GET_CLIENT_ID_REQ)
        return self.__send(code)

    def getServerInfo(self):
        code = pack('B',MSG_CODE_GET_SERVER_INFO_REQ)
        return self.__send(code)

    def ping(self):
        code = pack('B',MSG_CODE_PING_REQ)
        return self.__send(code)

    # ------------------------------------------------------------------
    # Object/Key Operations .. get(fetch), put(store), delete
    # ------------------------------------------------------------------
    def get(self,bucket,key, **kwargs):
        code = pack('B',MSG_CODE_GET_REQ)
        request = RpbGetReq()
        request.bucket = bucket
        request.key = key

        if 'r' in kwargs            : request.r = self._resolveNums(kwargs['r'])
        if 'pr' in kwargs           : request.pr = self._resolveNums(kwargs['pr'])
        if 'basic_quorum' in kwargs : request.basic_quorum = kwargs['basic_quorum']
        if 'notfound_ok' in kwargs  : request.notfound_ok = kwargs['notfound_ok']
        if 'if_modified' in kwargs  : request.if_modified = kwargs['if_modified']
        if 'head' in kwargs         : request.head = kwargs['head']
        if 'deletedvclock' in kwargs: request.deletedvclock = kwargs['deletedvclock']

        return self.__send(code,request)

    def put_new(self,bucket,key,content, vclock = None, **kwargs):
        return put(bucket,key,content, vclock, kwargs)
    
    def put(self,bucket,key,content, vclock = None, **kwargs):
        code = pack('B',MSG_CODE_PUT_REQ)
        request = RpbPutReq()
        request.bucket = bucket
        request.key = key

        if isinstance(content, str):
            request.content.value = content
        else:
            # assume its a dict
            request.content.value = content['value'] # mandatory
            if 'content_type' in content    : request.content.content_type = content['content_type']
            if 'charset' in content         : request.content.charset = content['charset']
            if 'content_encoding' in content: request.content.content_encoding = content['content_encoding']
            if 'vtag' in content            : request.content.vtag = content['vtag']
            if 'last_mod' in content        : request.content.last_mod = content['last_mod']
            if 'last_mod_usecs' in content  : request.content.last_mod_usecs = content['last_mod_usecs']
            if 'deleted' in content         : request.content.deleted = content['deleted']

            # write links if there are any
            if 'links' in content and isinstance(content['links'], list):
                for l in content['links']:
                    link = request.content.links.add()
                    link.bucket,link.key,link.tag = l

            # usermeta
            if 'usermeta' in content and isinstance(content['usermeta'], list):
                for l in content['usermeta']:
                    usermeta = request.content.usermeta.add()
                    usermeta.key,usermeta.value = l

            # indexes
            if 'indexes' in content and isinstance(content['indexes'], list):
                for l in content['indexes']:
                    indexes = request.content.indexes.add()
                    indexes.key,indexes.value = l


        if 'w' in kwargs               : request.w = self._resolveNums(kwargs['w'])
        if 'dw' in kwargs              : request.dw = self._resolveNums(kwargs['dw'])
        if 'return_body' in kwargs     : request.return_body = kwargs['return_body']
        if 'pw' in kwargs              : request.pw = self._resolveNums(kwargs['pw'])
        if 'if_modified' in kwargs     : request.if_modified = kwargs['if_modified']
        if 'if_not_modified' in kwargs : request.if_not_modified = kwargs['if_not_modified']
        if 'if_none_match' in kwargs   : request.if_none_match = kwargs['if_none_match']
        if 'return_head' in kwargs     : request.return_head = kwargs['return_head']

        if vclock:
            request.vclock = vclock

        return self.__send(code,request)

    def delete(self,bucket,key, **kwargs):
        code = pack('B',MSG_CODE_DEL_REQ)
        request = RpbDelReq()
        request.bucket = bucket
        request.key = key

        if 'vclock' in kwargs and kwargs['vclock']:
            request.vclock = kwargs['vclock']
        if 'rw' in kwargs     : request.rw = self._resolveNums(kwargs['rw'])
        if 'r' in kwargs      : request.r = self._resolveNums(kwargs['r'])
        if 'w' in kwargs      : request.w = self._resolveNums(kwargs['w'])
        if 'pr' in kwargs     : request.pr = self._resolveNums(kwargs['pr'])
        if 'pw' in kwargs     : request.pw = self._resolveNums(kwargs['pw'])
        if 'dw' in kwargs     : request.dw = self._resolveNums(kwargs['dw'])

        return self.__send(code,request)


    # ------------------------------------------------------------------
    # Bucket Operations .. getKeys, getBuckets, get/set Bucket properties
    # ------------------------------------------------------------------
    def getKeys(self, bucket):
        """
        operates different than the other messages, as it returns more than
        one respone .. see stringReceived() for handling
        """
        code = pack('B',MSG_CODE_LIST_KEYS_REQ)
        request = RpbListKeysReq()
        request.bucket = bucket
        self.__keyList = []
        return self.__send(code,request)

    def getBuckets(self):
        """
        operates different than the other messages, as it returns more than
        one respone .. see stringReceived() for handling
        """
        code = pack('B',MSG_CODE_LIST_BUCKETS_REQ)
        return self.__send(code)

    def getBucketProperties(self, bucket):
        code = pack('B',MSG_CODE_GET_BUCKET_REQ)
        request = RpbGetBucketReq()
        request.bucket = bucket
        return self.__send(code + request.SerializeToString())

    def setBucketProperties(self, bucket, **kwargs):
        code = pack('B',MSG_CODE_SET_BUCKET_REQ)
        request = RpbSetBucketReq()
        request.bucket = bucket

        if 'n_val' in kwargs      : request.props.n_val = kwargs['n_val']
        if 'allow_mult' in kwargs : request.props.allow_mult = kwargs['allow_mult']

        return self.__send(code,request)


    # ------------------------------------------------------------------
    # helper functions, message parser
    # ------------------------------------------------------------------
    def connectionMade(self):
        """
        return the protocol instance to the factory so it
        can be used directly
        """
        self.factory.connected.callback(self)

    def setTimeout(self,t):
        self.timeout = t

    def __send(self, code, request=None):
        """
        helper method for logging, sending and returning the deferred
        """
        if self.debug:
            print "[%s] %s %s" % (self.__class__.__name__,  request.__class__.__name__, str(request).replace('\n',' ' ))
        if request:
            msg = code + request.SerializeToString()
        else:
            msg = code
        self.sendString(msg)
        self.factory.d = Deferred()
        if self.timeout:
            self.timeoutd = reactor.callLater(self.timeout, self._triggerTimeout)

        return self.factory.d

    def _triggerTimeout(self):
        if not self.factory.d.called:
            try:
                self.factory.d.errback(RiakPBCException('timeout'))
            except Exception, e:
                print "Unable to handle Timeout: %s" % e

    def stringReceived(self, data):
        """
        messages contain as first byte a message type code that is used
        to map to the correct message type in self.riakResponses

        messages that dont have a body to parse return True, those are
        listed in self.nonMessages
        """
        if self.timeoutd and not self.timeoutd.called:
            self.timeoutd.cancel()  # stop timeout from beeing raised

        # decode messagetype
        code = unpack('B',data[:1])[0]
        if self.debug:
            print "[%s] stringReceived code %s" % (self.__class__.__name__,self.PBMessageTypes[code])

        if code not in self.riakResponses and code not in self.nonMessages:
            raise RiakPBCException('unknown messagetype: %d' % code)

        elif code in self.nonMessages:
            # for instance ping doesnt have a message, so we just return True
            if self.debug:
                print "[%s] stringReceived empty message type %s" % (self.__class__.__name__, self.PBMessageTypes[code])
            if not self.factory.d.called:
                self.factory.d.callback(True)
            return

        elif code == MSG_CODE_LIST_KEYS_RESP:
            # listKeys is special as it returns multiple response messages
            # each message can contain multiple keys
            # the last message contains a optional field "done"
            # so collect all the messages until the last one, then call the
            # callback
            response = RpbListKeysResp()
            response.ParseFromString(data[1:])
            if self.debug:
                print "[%s] %s %s" % (self.__class__.__name__,  response.__class__.__name__, str(response).replace('\n',' ' ))

            self.__keyList.extend([x for x in response.keys])
            if response.HasField('done') and response.done:
                if not self.factory.d.called:
                    self.factory.d.callback(self.__keyList)
                    self.__keyList = []

        else:
            # normal handling, pick the message code, call ParseFromString()
            # on it, and return the message
            response = self.riakResponses[code]()
            if len(data) > 1:
                # if there's data, parse it, otherwise return empty object
                response.ParseFromString(data[1:])
                if self.debug:
                    print "[%s] %s %s" % (self.__class__.__name__,  response.__class__.__name__, str(response).replace('\n',' ' ))

                if code == MSG_CODE_ERROR_RESP:
                    raise RiakPBCException('%s (%d)' % (response.errmsg, response.errcode))

            if not self.factory.d.called:
                self.factory.d.callback(response)

    def _resolveNums(self,val):
        if isinstance(val, str):
            val = val.lower()
            if val in self.rwNums:
                return self.rwNums[val]
            else:
                raise RiakPBCException('invalid value %s' % (val))
        else:
            return val


    @defer.inlineCallbacks
    def quit(self):
        yield self.transport.loseConnection()


class RiakPBCClientFactory(ClientFactory):

    protocol = RiakPBC
    noisy    = False

    def __init__(self):
        self.d = Deferred()
        self.connected = Deferred()

class RiakPBCClient(object):

    def connect(self,host,port):
        factory = RiakPBCClientFactory()
        reactor.connectTCP(host, port, factory)
        return factory.connected

