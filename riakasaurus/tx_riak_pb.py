from twisted.protocols.basic import Int32StringReceiver
from twisted.internet.protocol import ClientFactory
from twisted.internet.defer import Deferred
from twisted.internet import reactor

from struct import pack, unpack

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

riakResponses = {
    MSG_CODE_ERROR_RESP           : RpbErrorResp,
    MSG_CODE_GET_CLIENT_ID_RESP   : RpbGetClientIdResp,
    MSG_CODE_GET_SERVER_INFO_RESP : RpbGetServerInfoResp,
    MSG_CODE_GET_RESP             : RpbGetResp,
    MSG_CODE_PUT_RESP             : RpbPutResp,
    MSG_CODE_GET_SERVER_INFO_RESP : RpbGetServerInfoResp,
    }

nonMessages = (MSG_CODE_PING_RESP,
               MSG_CODE_DEL_RESP,
               MSG_CODE_SET_CLIENT_ID_RESP)


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

    debug = 0


    def connectionMade(self):
        """
        return the protocol instance to the factory so it
        can be used directly
        """
        self.factory.connected.callback(self)

    def __send(self, msg):
        """
        helper method for logging, sending and returning the deferred
        """
        if self.debug:
            print "[%s] %s" % (self.__class__.__name__, toHex(msg))
        self.sendString(msg)
        self.factory.d = Deferred()
        return self.factory.d
        
    def setClientId(self,clientId):
        code = pack('B',MSG_CODE_SET_CLIENT_ID_REQ)
        request = RpbSetClientIdReq()
        request.client_id = clientId
        return self.__send( code + request.SerializeToString())        
        
    def getClientId(self):
        code = pack('B',MSG_CODE_GET_CLIENT_ID_REQ)
        return self.__send(code)        
        
    def getServerInfo(self):
        code = pack('B',MSG_CODE_GET_SERVER_INFO_REQ)
        return self.__send(code)        
        
    def ping(self):
        code = pack('B',MSG_CODE_PING_REQ)
        return self.__send(code)        

    def getKeys(self, bucket):
        code = pack('B',MSG_CODE_LIST_KEYS_REQ)
        request = RpbList()
        request.client_id = clientId
    
        
    def get(self,bucket,key, **kwargs):
        code = pack('B',MSG_CODE_GET_REQ)
        request = RpbGetReq()
        request.bucket = bucket
        request.key = key

        if kwargs.get('r')            : request.r = kwargs['r']
        if kwargs.get('pr')           : request.pr = kwargs['pr']
        if kwargs.get('basic_quorum') : request.basic_quorum = kwargs['basic_quorum']
        if kwargs.get('notfound_ok')  : request.notfound_ok = kwargs['notfound_ok']
        if kwargs.get('if_modified')  : request.if_modified = kwargs['if_modified']
        if kwargs.get('head')         : request.head = kwargs['head']
        if kwargs.get('deletedvclock'): request.deletedvclock = kwargs['deletedvclock']

        return self.__send(code + request.SerializeToString())        
        

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
            if content.get('content_type'): request.content.content_type = content['content_type']
            if content.get('charset'): request.content.charset = content['charset']
            if content.get('content_encoding'): request.content.content_encoding = content['content_encoding']
            if content.get('vtag'): request.content.vtag = content['vtag']
            if content.get('last_mod'): request.content.last_mod = content['last_mod']
            if content.get('last_mod_usecs'): request.content.last_mod_usecs = content['last_mod_usecs']
            if content.get('deleted'): request.content.deleted = content['deleted']

            # write links if there are any
            if content.get('links') and isinstance(content['links'], list):
                for l in content['links']:
                    link = request.content.links.add()
                    link.bucket,link.key,link.tag = l

        if kwargs.get('w')               : request.w = kwargs['w']
        if kwargs.get('dw')              : request.dw = kwargs['dw']
        if kwargs.get('return_body')     : request.return_body = kwargs['return_body']
        if kwargs.get('pw')              : request.pw = kwargs['pw']
        if kwargs.get('if_modified')     : request.if_modified = kwargs['if_modified']
        if kwargs.get('if_not_modified') : request.if_not_modified = kwargs['if_not_modified']
        if kwargs.get('if_none_match')   : request.if_none_match = kwargs['if_none_match']
        if kwargs.get('return_head')     : request.return_head = kwargs['return_head']
        
        if vclock:
            request.vclock = vclock

        return self.__send(code + request.SerializeToString())

    def delete(self,bucket,key, **kwargs):
        code = pack('B',MSG_CODE_DEL_REQ)
        request = RpbDelReq()
        request.bucket = bucket
        request.key = key

        if kwargs.get('rw')     : request.rw = kwargs['rw']
        if kwargs.get('vclock') : request.vclock = kwargs['vclock']
        if kwargs.get('r')      : request.r = kwargs['r']
        if kwargs.get('w')      : request.w = kwargs['w']
        if kwargs.get('pr')     : request.pr = kwargs['pr']
        if kwargs.get('pw')     : request.pw = kwargs['pw']
        if kwargs.get('dw')     : request.dw = kwargs['dw']

        return self.__send(code + request.SerializeToString())
    
    
    def stringReceived(self, data):
        if self.debug:
            print "RiakPBC.stringReceived: ", toHex(data)
        code = unpack('B',data[:1])[0] # decode messagetype
        if code not in riakResponses and code not in nonMessages:
            raise RiakPBCException('unknown messagetype: %d' % code)

        if code in nonMessages:
            # for instance ping doesnt have a message
            self.factory.d.callback(True)
            return

        response = riakResponses[code]()
        if data[1:]:
            # if there's data, parse it, otherwise return empty object
            response.ParseFromString(data[1:])
            if code == MSG_CODE_ERROR_RESP:
                raise RiakPBCException('%s (%d)' % (response.errmsg, response.errcode))
            
        self.factory.d.callback(response)

    def quit(self):
        self.transport.loseConnection()


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
        
