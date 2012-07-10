"""
.. module:: mapreduce.py
"""

import json
from twisted.internet import defer

from riakasaurus import util, riak_link


class RiakMapReduce(object):
    """
    The RiakMapReduce object allows you to build up and run a
    map/reduce operation on Riak.
    """

    def __init__(self, client):
        """
        Initialize a Map/Reduce object.
        :param client: a RiakClient object.
        :returns: Nothing
        """
        self._client = client
        self._phases = []
        self._inputs = []
        self._input_mode = None
        self._mapreduce = None
        return None

    def search(self, bucket, query):
        """
        Perform a map-reduce operation using Riak Search
        :param bucket: Name of the bucket with Riak Search enabled
        :param query: Riak Search query
        """
        self._input_mode = 'query'
        self._inputs = {'module':'riak_search',
                       'function':'mapred_search',
                       'arg':[bucket, query]}
        return self

    def set_mapreduce(self, mapreduce):
        """
        Manually set a map/reduce query without assembling it.
        """
        self._mapreduce = mapreduce
        return self

    def index(self, bucket, index, startkey, endkey = None):
        """
        Begin a map/reduce operation using a Secondary Index
        query.
        @param bucket - The bucket over which to perform the search.
        @param query - The search query.
        """
        self._input_mode = 'query'

        if endkey == None:
            self._inputs = {'bucket': bucket,
                            'index':index,
                            'key':startkey }
        else:
            self._inputs = {'bucket':bucket,
                            'index':index,
                            'start':startkey,
                            'end':endkey }
        return self

    def add(self, arg1, arg2=None, arg3=None):
        """
        Add inputs to a map/reduce operation. This method takes three
        different forms, depending on the provided inputs. You can
        specify either a RiakObject, a string bucket name, or a bucket,
        key, and additional arg.
        :param arg1: RiakObject or Bucket or Key Filter
        :param arg2: Key or blank
        :param arg3: Arg or blank
        :returns: RiakMapReduce

        Key filters are dictionaries of the form:

            {"bucket": "<bucket_name>",
             "key_filters": [ ["<operation>", "<arg1>", "<argN>"],
                             ]}
        """
        # avoid circular import
        from riakasaurus import riak_object
        if (arg2 == None) and (arg3 == None):
            if isinstance(arg1, riak_object.RiakObject):
                return self.add_object(arg1)
            elif isinstance(arg1, dict):
                return self.add_key_filter(arg1)
            else:
                return self.add_bucket(arg1)
        else:
            return self.add_bucket_key_data(arg1, arg2, arg3)

    def add_object(self, obj):
        """Set bucket key data with object."""
        return self.add_bucket_key_data(obj._bucket._name, obj._key, None)

    def add_bucket_key_data(self, bucket, key, data):
        """Add data to the bucket."""
        if self._input_mode == 'bucket' or \
           self._input_mode == 'key_filter':
            raise Exception(('Already added a %s, ' % self._input_mode) +
                            'can\'t add an object.')
        else:
            self._inputs.append([bucket, key, data])
            return self

    def add_bucket(self, bucket):
        """Set bucket to work with."""
        self._input_mode = 'bucket'
        self._inputs = bucket
        return self

    def add_key_filter(self, key_filter):
        """Set up key filter to use."""
        if self._input_mode == 'bucket':
            raise Exception('Already added a bucket can\'t add a key filter.')
        else:
            self._input_mode = "key_filter"
            self._inputs = key_filter
            return self

    def link(self, bucket='_', tag='_', keep=False):
        """
        Add a link phase to the map/reduce operation.
        :param bucket: Bucket name (default '_' means all buckets)
        :param tag: Tag (default '_' means all tags)
        :param keep: Boolean flag to keep results from this stage.
        :returns: self
        """
        self._phases.append(RiakLinkPhase(bucket, tag, keep))
        return self

    def map(self, function, options=None):
        """
        Add a map phase to the map/reduce operation.
        :param function: Function to run
        :param options: Optional dictionary.
        :returns: self

        The function can be either a named Javascript function:
            'Riak.mapValues'
        an anonymous JavaScript function:
            'function(...)  ... )'
        or a list:
            ['erlang_module', 'function']

        The options is an associative array containing:
            'language',
            'keep' flag, and/or
            'arg'

        .. todo:: Remove get_value and use {}.get()
        """
        if options == None:
            options = []

        if isinstance(function, list):
            language = 'erlang'
        else:
            language = 'javascript'

        mr = RiakMapReducePhase('map',
                                function,
                                util.get_value('language', options,
                                                     language),
                                util.get_value('keep', options,
                                                     False),
                                util.get_value('arg', options,
                                                     None))
        self._phases.append(mr)
        return self

    def reduce(self, function, options=None):
        """
        Add a reduce phase to the map/reduce operation.

        :param function: Function to run
        :param options: Optional dictionary.
        :returns: self

        The function can be either a named Javascript function:
            'Riak.mapValues'
        an anonymous JavaScript function:
            'function(...)  ... )'
        or a list:
            ['erlang_module', 'function']

        The optios is an associative array containing:
            'language',
            'keep' flag, and/or
            'arg'

        .. todo:: Remove get_value and use {}.get()
        """
        if options == None:
            options = []

        if isinstance(function, list):
            language = 'erlang'
        else:
            language = 'javascript'

        mr = RiakMapReducePhase('reduce',
                                function,
                                util.get_value('language', options,
                                                     language),
                                util.get_value('keep', options,
                                                     False),
                                util.get_value('arg', options,
                                                     None))
        self._phases.append(mr)
        return self

    @defer.inlineCallbacks
    def run(self, timeout=None):
        """
        Run the map/reduce operation. Returns a list of results, or an
        array of RiakLink objects if the last phase is a link phase.

        :param timeout: Timeout in seconds. Defaults to waiting forever.
        :returns: list of results  -- via deferred
        """

        if self._mapreduce:
            # explicit map/reduce has been set.
            content = self._mapreduce
        else:
            # Convert all phases to associative arrays. Also,
            # if none of the phases are accumulating, then set the last one to
            # accumulate.
            num_phases = len(self._phases)
            keep_flag = False
            query = []
            for i in range(num_phases):
                phase = self._phases[i]
                if (i == (num_phases - 1)) and (not keep_flag):
                    phase._keep = True
                if phase._keep:
                    keep_flag = True
                query.append(phase._to_array())

            # Construct the job, optionally set the timeout...
            job = {'inputs': self._inputs, 'query': query}
            if timeout != None:
                job['timeout'] = timeout

            content = json.dumps(job)

        # Do the request...
        host = self._client._host
        port = self._client._port
        url = "/" + self._client._mapred_prefix
        headers = {"Content-Type": "application/json"}
        response = yield util.http_request_deferred('POST', host, port,
                                                    url, headers, content)
        if response[0]["http_code"] > 299:
            raise Exception("Error running map/reduce job. Error: " +
                            response[1])
        result = json.loads(response[1])

        # If the last phase is NOT a link phase, then return the result.
        if self._phases:
            lastIsLink = isinstance(self._phases[-1], RiakLinkPhase)
        else:
            lastIsLink = False

        if not lastIsLink:
            defer.returnValue(result)

        # Otherwise, if the last phase IS a link phase, then convert the
        # results to RiakLink objects.
        a = []
        for r in result:
            link = riak_link.RiakLink(r[0], r[1], r[2])
            link._client = self._client
            a.append(link)

        defer.returnValue(a)


class RiakMapReducePhase(object):
    """
    The RiakMapReducePhase holds information about a Map phase or
    Reduce phase in a RiakMapReduce operation.
    """

    def __init__(self, type, function, language, keep, arg):
        """
        Construct a RiakMapReducePhase object.

        :param type: 'map' or 'reduce'
        :param function: string or array
        :param language: 'javascript' or 'erlang'
        :param keep: True to return the output of this phase in results.
        :param arg: Additional value to pass into the map or reduce function.
        :returns: None
        """
        self._type = type
        self._language = language
        self._function = function
        self._keep = keep
        self._arg = arg
        return None

    def _to_array(self):
        """
        Convert the RiakMapReducePhase to an associative array. Used
        internally.
        """
        stepdef = {'keep': self._keep,
                   'language': self._language,
                   'arg': self._arg}

        if ((self._language == 'javascript') and
              isinstance(self._function, list)):
            stepdef['bucket'] = self._function[0]
            stepdef['key'] = self._function[1]
        elif ((self._language == 'javascript') and
                isinstance(self._function, str)):
            if ("{" in self._function):
                stepdef['source'] = self._function
            else:
                stepdef['name'] = self._function

        elif (self._language == 'erlang' and isinstance(self._function, list)):
            stepdef['module'] = self._function[0]
            stepdef['function'] = self._function[1]

        return {self._type: stepdef}


class RiakLinkPhase(object):
    """
    The RiakLinkPhase object holds information about a Link phase in a
    map/reduce operation.
    """

    def __init__(self, bucket, tag, keep):
        """
        Construct a RiakLinkPhase object.
        @param string bucket - The bucket name.
        @param string tag - The tag.
        @param boolean keep - True to return results of this phase.
        """
        self._bucket = bucket
        self._tag = tag
        self._keep = keep
        return None

    def _to_array(self):
        """
        Convert the RiakLinkPhase to an associative array. Used
        internally.
        """
        stepdef = {'bucket': self._bucket,
                   'tag': self._tag,
                   'keep': self._keep}
        return {'link': stepdef}

