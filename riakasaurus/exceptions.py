class RequestTimeout(Exception):
    """
        Raised to notify that an operation exceeded its timeout.
    """


class ConnectTimeout(Exception):
    """
        Raised to notify that a connection timed out
    """


class RiakPBCException(Exception):
    """Generic PBC exception"""
    pass


class RiakError(Exception):
    """Generic Riak error"""
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
