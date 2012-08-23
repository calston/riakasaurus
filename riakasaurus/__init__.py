"""
riakasaurus:
Twisted module for communicating with the Riak data store from 
Basho Technologies, Inc. http://basho.com/

Modules based on code originally disbributed by Basho:
    http://riak.basho.com/python_client_api/riak.html
"""
VERSION = '1.0.6'

LICENSE = """
%s

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at
 
  http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

Riakasaurus is based on the txriak project

This module is an almost complete copy of the original riak.py provided
in Basho's standard Riak distribution. Only functional change is to use 
Twisted deferreds when communicating with Riak via http and to use 
Twisted logging.

Original riak.py was Apache License 2.0. That license is maintained for
the riakasaurus module. 

Copyrights from the original riak.py module are: 
Copyright 2010 Rusty Klophaus <rusty@basho.com>
Copyright 2010 Justin Sheehy <justin@basho.com>
Copyright 2009 Jay Baird <jay@mochimedia.com>

Thank you Basho for open sourcing your interface libraries. 

"""

class RiakError(Exception) :
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
