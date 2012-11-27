import tx_riak_pb
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from pprint import pformat

def stop(result):
    print "stop"
    print result
    reactor.stop()
    
def error(failure):
    print str(failure)
    import traceback,sys
    traceback.print_exc(file=sys.stdout)

def storeConnection(connection, store):
    print "storeConnection:", connection
    store['con'] = connection


def put(result, store):
    print "putting foo"
    return store['con'].put('bucket','key', 'foo')
    
def p(result):
    print pformat(result)

def get(result, store):
    print "calling RiakPBC.get"
    return store['con'].get('bucket','key')
    
client = tx_riak_pb.RiakPBCClient()
store = {}
d = client.connect('127.0.0.1', 8087)
d.addCallback(storeConnection, store)
d.addCallback(put, store)
d.addCallback(p)
d.addCallback(get, store)
d.addCallback(stop)
d.addErrback(error)
    
reactor.run()
