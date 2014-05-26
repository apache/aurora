#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import contextlib
import os
import sys
from socket import timeout as SocketTimeout

from twitter.common import log
from twitter.common.lang import Compatibility

if Compatibility.PY3:
  from http.client import HTTPException
  import urllib.request as urllib_request
  from urllib.error import URLError, HTTPError
else:
  from httplib import HTTPException
  import urllib2 as urllib_request
  from urllib2 import URLError, HTTPError


class HttpSignaler(object):
  """Simple HTTP endpoint wrapper to check health or trigger quitquitquit/abortabortabort"""
  TIMEOUT_SECS = 1.0
  FAILURE_REASON_LENGTH = 10

  class Error(Exception): pass
  class QueryError(Error): pass

  def __init__(self, port, host='localhost', timeout_secs=None):
    self._host = host
    self._url_base = 'http://%s:%d/' % (host, port)
    if timeout_secs is None:
      env_timeout = os.getenv('AURORA_HTTP_SIGNALER_TIMEOUT_SECS')
      if env_timeout is not None:
        log.info('Using timeout %s secs (from AURORA_HTTP_SIGNALER_TIMEOUT_SECS).' % env_timeout)
        self._timeout_secs = float(env_timeout)
      else:
        log.debug('Using timeout %s secs (default).' % self.TIMEOUT_SECS)
        self._timeout_secs = self.TIMEOUT_SECS
    else:
      log.debug('Using timeout %s secs.' % timeout_secs)
      self._timeout_secs = timeout_secs

  def url(self, endpoint):
    return self._url_base + endpoint

  @property
  def opener(self):
    return urllib_request.urlopen

  def query(self, endpoint, data=None):
    """Request an HTTP endpoint with a GET request (or POST if data is not None)"""
    url = self.url(endpoint)
    log.debug("%s: %s %s" % (self.__class__.__name__, 'GET' if data is None else 'POST', url))

    def raise_error(reason):
      raise self.QueryError('Failed to signal %s: %s' % (self.url(endpoint), reason))

    try:
      with contextlib.closing(
          self.opener(url, data, timeout=self._timeout_secs)) as fp:
        return fp.read()
    except (HTTPException, SocketTimeout) as e:
      # the type of an HTTPException is typically more useful than its contents (since for example
      # BadStatusLines are often empty). likewise with socket.timeout.
      raise_error('Error within %s' % e.__class__.__name__)
    except (URLError, HTTPError) as e:
      raise_error(e)
    except Exception as e:
      raise_error('Unexpected error: %s' % e)

  def __call__(self, endpoint, use_post_method=False, expected_response=None):
    """Returns a (boolean, string|None) tuple of (call success, failure reason)"""
    try:
      response = self.query(endpoint, '' if use_post_method else None).strip().lower()
      if expected_response is not None and response != expected_response:
        def shorten(string):
          return (string if len(string) < self.FAILURE_REASON_LENGTH
                         else "%s..." % string[:self.FAILURE_REASON_LENGTH - 3])
        reason = 'Response differs from expected response (expected "%s", got "%s")'
        log.warning(reason % (expected_response, response))
        return (False, reason % (shorten(str(expected_response)), shorten(str(response))))
      else:
        return (True, None)
    except self.QueryError as e:
      return (False, str(e))

  def health(self):
    return self('health', use_post_method=False, expected_response='ok')

  def quitquitquit(self):
    return self('quitquitquit', use_post_method=True)

  def abortabortabort(self):
    return self('abortabortabort', use_post_method=True)
