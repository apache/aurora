import contextlib
from socket import timeout as SocketTimeout
import sys

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

# TODO(wickman) This is an abstraction leak -- this should be fixed upstream by
# MESOS-3710
import socks


class HttpSignaler(object):
  """Simple HTTP endpoint wrapper to check health or trigger quitquitquit/abortabortabort"""
  TIMEOUT_SECS = 1.0
  FAILURE_REASON_LENGTH = 10

  class Error(Exception): pass
  class QueryError(Error): pass

  def __init__(self, port, host='localhost', timeout_secs=TIMEOUT_SECS):
    self._host = host
    self._url_base = 'http://%s:%d/' % (host, port)
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
    try:
      with contextlib.closing(
          self.opener(url, data, timeout=self._timeout_secs)) as fp:
        return fp.read()
    except (URLError, HTTPError, HTTPException, SocketTimeout, socks.GeneralProxyError) as e:
      # the type of an HTTPException is typically more useful than its contents (since for example
      # BadStatusLines are often empty). likewise with socket.timeout.
      err = e.__class__.__name__ if isinstance(e, (HTTPException, SocketTimeout)) else e
      reason = 'Failed to signal %s: %s' % (self.url(endpoint), err)
      raise self.QueryError(reason)

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
