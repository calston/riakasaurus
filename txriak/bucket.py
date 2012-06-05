"""
.. module:: bucket.py

Riak bucket objects

"""

import urllib
import json
from twisted.internet import defer

from riakasaurus import riak_object, util


class RiakBucket(object):
    """
    The RiakBucket object allows you to access and change information
    about a Riak bucket, and provides methods to create or retrieve
    objects within the bucket.
    """

    SEARCH_PRECOMMIT_HOOK = {"mod": "riak_search_kv_hook", "fun": "precommit"}

    def __init__(self, client, name):
        """
        Initialize RiakBucket
        """
        self._client = client
        self._name = name
        self._r = None
        self._w = None
        self._dw = None
        return None

    def get_name(self):
        """
        Get the bucket name.
        .. todo:: remove accessors
        """
        return self._name

    def get_r(self, r=None):
        """
        Get the R-value for this bucket, if it is set, otherwise return
        the R-value for the client.
        :rtype: return integer
        .. todo:: make into property
        """
        if (r != None):
            return r
        if (self._r != None):
            return self._r
        return self._client.get_r()

    def set_r(self, r):
        """
        Set the R-value for this bucket. get(...) and get_binary(...)
        operations that do not specify an R-value will use this value.
        @param integer r - The new R-value.
        @return self
        .. todo:: convert to property
        """
        self._r = r
        return self

    def get_w(self, w):
        """
        Get the W-value for this bucket, if it is set, otherwise return
        the W-value for the client.
        @return integer
        .. todo:: convert to property
        """
        if (w != None):
            return w
        if (self._w != None):
            return self._w
        return self._client.get_w()

    def set_w(self, w):
        """
        Set the W-value for this bucket. See set_r(...) for more information.
        @param integer w - The new W-value.
        @return self
        .. todo:: convert to property
        """
        self._w = w
        return self

    def get_dw(self, dw):
        """
        Get the DW-value for this bucket, if it is set, otherwise return
        the DW-value for the client.
        @return integer
        .. todo:: convert to property
        """
        if (dw != None):
            return dw
        if (self._dw != None):
            return self._dw
        return self._client.get_dw()

    def set_dw(self, dw):
        """
        Set the DW-value for this bucket. See set_r(...) for more information.
        @param integer dw - The new DW-value
        @return self
        .. todo:: convert to property
        """
        self._dw = dw
        return self

    def new(self, key, data=None):
        """
        Create a new Riak object that will be stored as JSON.

        :param key: Name of the key.
        :param data: The data to store.
        :returns: riak_object.RiakObject
        """
        obj = riak_object.RiakObject(self._client, self, key)
        obj.set_data(data)
        obj.set_content_type('text/json')
        obj._jsonize = True
        return obj

    def new_binary(self, key, data, content_type='text/json'):
        """
        Create a new Riak object that will be stored as plain text/binary.

        :param key: Name of the key.
        :param data: The data to store.
        :param content_type: The content type of the object.
        :returns: riak_object.RiakObject
        """
        obj = riak_object.RiakObject(self._client, self, key)
        obj.set_data(data)
        obj.set_content_type(content_type)
        obj._jsonize = False
        return obj

    @defer.inlineCallbacks
    def get(self, key, r=None):
        """
        Retrieve a JSON-encoded object from Riak.

        :param key: Name of the key.
        :param r: R-Value of this request (defaults to bucket's R)
        :returns: riak_object.RiakObject -- via deferred
        """
        obj = riak_object.RiakObject(self._client, self, key)
        obj._jsonize = True
        r = self.get_r(r)
        result = yield obj.reload(r)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def get_binary(self, key, r=None):
        """
        Retrieve a binary/string object from Riak.

        :param key: Name of the key.
        :param r: R-Value of this request (defaults to bucket's R)
        :returns: riak_object.RiakObject -- via deferred
        """
        obj = riak_object.RiakObject(self._client, self, key)
        obj._jsonize = False
        r = self.get_r(r)
        result = yield obj.reload(r)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def set_n_val(self, nval):
        """
        Set the N-value for this bucket, which is the number of replicas
        that will be written of each object in the bucket. Set this once
        before you write any data to the bucket, and never change it
        again, otherwise unpredictable things could happen. This should
        only be used if you know what you are doing.
        @param integer nval - The new N-Val.
        .. todo:: Given the danger, some way to first check for existence?
        """
        result = yield self.set_property('n_val', nval)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def get_n_val(self):
        """
        Retrieve the N-value for this bucket.
        @return integer
        .. todo:: what happens if you ask for n_val before ever writing?
        """
        result = yield self.get_property('n_val')
        defer.returnValue(result)

    @defer.inlineCallbacks
    def set_allow_multiples(self, the_bool):
        """
        If set to True, then writes with conflicting data are stored
        and returned to the client. This situation can be detected by
        calling has_siblings() and get_siblings(). This should only be used
        if you know what you are doing.

        :param the_bool: True to store and return conflicting writes.
        :returns: deferred
        """
        result = yield self.set_property('allow_mult', the_bool)

        defer.returnValue(result)

    @defer.inlineCallbacks
    def get_allow_multiples(self):
        """
        Retrieve the 'allow multiples' setting.

        :returns: Boolean -- via deferred
        """
        result = yield self.get_property('allow_mult')

        defer.returnValue(result)

    @defer.inlineCallbacks
    def set_property(self, key, value):
        """
        Set a bucket property. This should only be used if you know what
        you are doing.

        :param key: property to set.
        :param value: property value to set.
        :returns: deferred result
        """
        result = yield self.set_properties({key: value})

        defer.returnValue(result)

    @defer.inlineCallbacks
    def get_property(self, key):
        """
        Retrieve a bucket property.

        :param key: property to retrieve
        :returns: property value -- as deferred
        """

        props = yield self.get_properties()

        if (key in props.keys()):
            defer.returnValue(props[key])
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def search_enabled(self):
        """
        Returns True if the search precommit hook is enabled for this bucket.
        """
        props = yield self.get_property("precommit") or []
        defer.returnValue(self.SEARCH_PRECOMMIT_HOOK in props)

    @defer.inlineCallbacks
    def enable_search(self):
        precommit_hooks = yield self.get_property("precommit") or []

        if self.SEARCH_PRECOMMIT_HOOK not in precommit_hooks:
            yield self.set_properties({"precommit":
                precommit_hooks + [self.SEARCH_PRECOMMIT_HOOK]})

        defer.returnValue(True)

    @defer.inlineCallbacks
    def disable_search(self):
        precommit_hooks = yield self.get_property("precommit") or []

        if self.SEARCH_PRECOMMIT_HOOK in precommit_hooks:
            precommit_hooks.remove(self.SEARCH_PRECOMMIT_HOOK)
            yield self.set_properties({"precommit": precommit_hooks})

        defer.returnValue(True)

    def search(self, query, **params):
        return self._client.solr().search(self._name, query, **params)

    @defer.inlineCallbacks
    def set_properties(self, props):
        """
        Set multiple bucket properties in one call. This should only be
        used if you know what you are doing.

        :param props: a dictionary of keys and values to store.
        :returns: deferred
        """
        host, port, url = util.build_rest_path(self._client, self)
        headers = {'Content-Type': 'application/json'}
        content = json.dumps({'props': props})

        #Run the request...
        response = yield util.http_request_deferred('PUT', host,
                                                         port, url,
                                                         headers, content)

        # Handle the response...
        if (response == None):
            raise Exception('Error setting bucket properties.')

        # Check the response value...
        status = response[0]['http_code']
        if (status != 204):
            raise Exception('Error setting bucket properties.')
        defer.returnValue(response)

    @defer.inlineCallbacks
    def get_properties(self):
        """
        Retrieve a dictionary of all bucket properties.

        :returns: dictionary -- via deferred
        """

        # Run the request...
        params = {'props': 'True', 'keys': 'False'}
        host, port, url = util.build_rest_path(self._client, self,
                                                    None, None, params)
        response = yield util.http_request_deferred('GET', host,
                                                         port, url)

        # Use a riak_object.RiakObject to interpret the response.
        # We are just interested in the value.
        obj = riak_object.RiakObject(self._client, self, None)
        obj._populate(response, [200])
        if (not obj.exists()):
            raise Exception('Error getting bucket properties.')

        props = obj.get_data()
        props = props['props']
        defer.returnValue(props)

    @defer.inlineCallbacks
    def list_keys(self):
        """
        Retrieve a list of all bucket keys.

        :returns: list -- via deferred
        """
        # Create the request
        params = {"keys": "stream", "props": "false"}

        host, port, url = util.build_rest_path(self._client,
                                                    self,
                                                    None,
                                                    None,
                                                    params)
        raw_response = yield util.http_request_deferred('GET', host,
                                                             port, url)

        if raw_response[0]["http_code"] != 200:
            raise Exception('Error listing keys in bucket %s.' % self._name)

        # Hacky method to deal with the concatenated response
        # we get due to the chunked encoding method.
        parts = raw_response[1].split('{"keys":[')
        keys = []

        for part in parts[1:]:
            temp_keys = json.loads('{"keys":[' + part)["keys"]
            keys = keys + temp_keys

        defer.returnValue([urllib.unquote(x) for x in keys])

    def get_keys(self):
        return self.list_keys()

    @defer.inlineCallbacks
    def purge_keys(self):
        """
        Purge all keys from the bucket.

        :returns: None

        This is a convenience function that lists all of the keys
        in the bucket and then deletes them.

        NB: This is a VERY resource-intensive operation, and is
            IRREVERSIBLE. Be careful.
        """

        # Get the current key list
        keys = yield self.list_keys()

        # Major key-killing action
        for key in keys:
            obj = yield self.get_binary(key)
            yield obj.delete()
