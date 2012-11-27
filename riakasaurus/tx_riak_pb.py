from twisted.protocols.basic import Int32StringReceiver
from twisted.internet.protocol import ClientFactory
from twisted.internet.defer import Deferred
from twisted.internet import reactor

from struct import pack, unpack

from riak_kv_pb2 import RpbGetReq, RpbGetResp, RpbPutReq, RpbPutResp

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

RiakMessages = {MSG_CODE_GET_REQ  : RpbGetReq,
                MSG_CODE_GET_RESP : RpbGetResp,
                MSG_CODE_PUT_REQ  : RpbPutReq,
                MSG_CODE_PUT_RESP : RpbPutResp,
                }

def toHex(s):
    lst = []
    for ch in s:
        hv = hex(ord(ch)).replace('0x', '')
        if len(hv) == 1:
            hv = '0'+hv
        lst.append(hv+ ' ')
    
    return reduce(lambda x,y:x+y, lst)

class RiakPBC(Int32StringReceiver):

    debug = 1

    def connectionMade(self):
        """Return self by deferred"""
        print "RiakPBC.connectionMade"
        self.factory.connected.callback(self)

        
    def get(self,bucket,key):
        code = pack('B',MSG_CODE_GET_REQ)
        request = RpbGetReq()
        request.bucket = bucket
        request.key = key

        if self.debug:
            print "RiakPBC.get sendString %s" % toHex(code + request.SerializeToString())
        self.sendString(code + request.SerializeToString())
        self.factory.d = Deferred()
        return self.factory.d

    def put(self,bucket,key,content):
        code = pack('B',MSG_CODE_PUT_REQ)
        request = RpbPutReq()
        request.bucket = bucket
        request.key = key
        request.content.value = content

        if self.debug:
            print "RiakPBC.put sendString %s" % toHex(code + request.SerializeToString())
        self.sendString(code + request.SerializeToString())
        self.factory.d = Deferred()
        return self.factory.d
    
    def stringReceived(self, data):
        if self.debug:
            print "RiakPBC.stringReceived: ", toHex(data)
        code = unpack('B',data[:1])[0] # decode messagetype
        if code not in RiakMessages:
            raise Exception('unknown messagetype: %d' % code)

        response = RiakMessages[code]()
        if data[1:]:
            # if there's data, parse it, otherwise return empty object
            response.ParseFromString(data[1:])
        self.factory.d.callback(response)


class RiakPBCClientFactory(ClientFactory):

    protocol = RiakPBC

    def __init__(self):
        self.d = Deferred()
        self.connected = Deferred()

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason        

class RiakPBCClient(object):
    
    def connect(self,host,port):
        factory = RiakPBCClientFactory()
        reactor.connectTCP(host, port, factory)
        return factory.connected
        
