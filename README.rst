Riakasaurus
===========

A Riak client library for Twisted

Riakasaurus is based on my fork of txriak from https://bitbucket.org/asi/txriak, which in turn draws most of it's work from the Riak python library by Basho. 

Installation
-----

From source 
    # git clone git://github.com/calston/riakasaurus.git
    # cd riakasaurus
    # python setup.py install

From PyPi
    # easy_install riakasauru


Usage
-----

Riakasaurus uses inline deferreds, any operation that requires connecting to Riak will return a deferred. The usuage follows from the standard python library as much as possible::

    from twisted.internet import reactor
    from riakasaurus import riak 

    # Create a client object

    client = riak.RiakClient()

    # Create a bucket object
    bucket = client.bucket('my_bucket') 
    
    # Create a new object
    object = bucket.new('penguins', {'colors': ['white', 'black']})

    def stored(*a):
        print "Object stored"
        reactor.stop()

    # Store returns a deferred, it will create the bucket if needed then store the object
    object.store().addCallback(stored)

    reactor.run()

Contributors 
---

Thank you to these people for their previous contributions to txriak and Riakasaurus 

 * Simon Cross - Solr search and restructuring many things 
 * tobixx (https://github.com/tobixx) - returnbody changes
 * Jason J. W. Williams 
 * Appropriate Solutions - For the origional txriak work
