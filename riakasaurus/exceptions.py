class RequestTimeout(Exception):
    """ 
        Raised to notify that an operation exceeded its timeout. 
    """

class ConnectTimeout(Exception):
    """ 
        Raised to notify that a connection timed out
    """

class RiakPBCException(Exception):
    """ Generic PBC exception"""
    pass

