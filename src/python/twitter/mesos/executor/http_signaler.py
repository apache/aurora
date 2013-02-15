import contextlib
from socket import timeout as SocketTimeout

from twitter.common import log
from twitter.common.lang import Compatibility

if Compatibility.PY3:
  from http.client import HTTPException
  from urllib.request import urlopen
  from urllib.error import URLError, HTTPError
else:
  from httplib import HTTPException
  from urllib2 import urlopen, URLError, HTTPError


class HttpSignaler(object):
  """Simple HTTP endpoint wrapper to check health or trigger quitquitquit/abortabortabort"""
  TIMEOUT_SECS = 1.0
  FAILURE_REASON_LENGTH = 10

  def __init__(self, port, host='localhost'):
    self._url_base = 'http://%s:%d/' % (host, port)

  def url(self, endpoint):
    return self._url_base + endpoint

  def __call__(self, endpoint, use_post_method, expected_response=None):
    """Returns a (boolean, string|None) tuple of (call success, failure reason)"""
    try:
      data = '' if use_post_method else None
      with contextlib.closing(urlopen(self.url(endpoint), data, timeout=self.TIMEOUT_SECS)) as fp:
        response = fp.read().strip().lower()
        if expected_response is not None and response != expected_response:
          def shorten(string):
            return (string if len(string) < self.FAILURE_REASON_LENGTH
                           else "%s..." % string[:self.FAILURE_REASON_LENGTH - 3])
          reason = 'Response differs from expected response (expected "%s", got "%s")'
          log.warning(reason % (expected_response, response))
          return (False, reason % (shorten(str(expected_response)), shorten(str(response))))
        else:
          return (True, None)
    except (URLError, HTTPError, HTTPException) as e:
      # the type of an HTTPException is typically more useful than its contents (since for example
      # BadStatusLines are often empty). likewise with socket.timeout.
      err = e.__class__.__name__ if isinstance(e, (HTTPException, SocketTimeout)) else e
      reason = 'Failed to signal %s: %s' % (self.url(endpoint), err)
      return (False, reason)

  def health(self):
    return self('health', use_post_method=False, expected_response='ok')

  def quitquitquit(self):
    return self('quitquitquit', use_post_method=True)

  def abortabortabort(self):
    return self('abortabortabort', use_post_method=True)
