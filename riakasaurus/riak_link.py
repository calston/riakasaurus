"""
.. module:: link.py

"""

import urllib


class RiakLink(object):
    """
    The RiakLink object represents a link from one Riak object to
    another.
    @package RiakLink
    """

    def __init__(self, bucket, key, tag=None):
        """
        Construct a RiakLink object.

        :param bucket: The bucket name
        :param key: The key name
        :param tag: The tag name
        :returns: None
        """
        self._bucket = bucket
        self._key = key
        self._tag = tag
        self._client = None
        return None

    def get(self, r=None):
        """
        Retrieve the RiakObject to which this link points.

        :param r: R-value to use for this call.
        :returns: RiakObject

        .. todo:: this looks wrong. r should be override of r, not _key.
        """
        return self._client.bucket(self._bucket).get(self._key, r)

    def get_binary(self, r=None):
        """
        Retrieve the RiakObject to which this link points, as a binary.

        :param r: R-value to use for this call.
        :returns: RiakObject

        .. todo:: this looks wrong. r should be override of r, not _key.
        """
        return self._client.bucket(self._bucket).get_binary(self._key, r)

    def get_bucket(self):
        """
        Get the bucket name of this link.

        :returns: bucket name, not the object

        .. todo:: remove accessor
        """
        return self._bucket

    def set_bucket(self, name):
        """
        Set the bucket name of this link.
        @param string name - The bucket name.
        @return self

        .. todo:: remove accessor
        """
        self._bucket = name
        return self

    def get_key(self):
        """
        Get the key of this link.
        @return string

        .. todo:: remove accessor
        """
        return self._key

    def set_key(self, key):
        """
        Set the key of this link.
        @param string key - The key.
        @return self
        .. todo:: remove accessor
        """
        self._key = key
        return self

    def get_tag(self):
        """
        Get the tag of this link.
        @return string
        .. todo:: convert to a property
        """
        if (self._tag == None):
            return self._bucket
        else:
            return self._tag

    def set_tag(self, tag):
        """
        Set the tag of this link.
        @param string tag - The tag.
        @return self
        .. todo:: convert to a property
        """
        self._tag = tag
        return self

    def _to_link_header(self, client):
        """
        Convert this RiakLink object to a link header string. Used internally.
        """
        link = ''
        link += '</'
        link += client._prefix + '/'
        link += urllib.quote_plus(self._bucket) + '/'
        link += urllib.quote_plus(self._key) + '>; riaktag="'
        link += urllib.quote_plus(self.get_tag()) + '"'
        return link

    def isEqual(self, link):
        """
        Return True if this link and self are equal.

        :param link: RiakLink object
        :returns: True if equal
        """
        is_equal = ((self._bucket == link._bucket) and
                     (self._key == link._key) and
                     (self.get_tag() == link.get_tag())
                   )
        return is_equal

if __name__ == "__main__":
    pass
