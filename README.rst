Riakasaurus
===========

The Riak client library for Twisted with a silly name.

|riakasaurus-ci|_

.. |riakasaurus-ci| image:: https://travis-ci.org/calston/riakasaurus.png?branch=master
.. _riakasaurus-ci: https://travis-ci.org/calston/riakasaurus

Installation
-----

From source 
    # git clone git://github.com/calston/riakasaurus.git
    # cd riakasaurus
    # python setup.py install

From PyPi
    # easy_install riakasaurus

From pip
    # pip install riakasaurus


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

