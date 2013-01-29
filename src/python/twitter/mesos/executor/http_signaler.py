import contextlib

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
  TIMEOUT_SECS = 1.0

  def __init__(self, port, host='localhost'):
    self._url_base = 'http://%s:%d/' % (host, port)

  def url(self, endpoint):
    return self._url_base + endpoint

  def __call__(self, endpoint, use_post_method, expected_response=None):
    try:
      data = '' if use_post_method else None
      with contextlib.closing(urlopen(self.url(endpoint), data, timeout=self.TIMEOUT_SECS)) as fp:
        response = fp.read().strip().lower()
        return response == expected_response if expected_response is not None else True
    except (URLError, HTTPError, HTTPException) as e:
      log.warning('Failed to signal %s: %s' % (self.url(endpoint), e))
      return False

  def health(self):
    return self('health', use_post_method=False, expected_response='ok')

  def quitquitquit(self):
    return self('quitquitquit', use_post_method=True)

  def abortabortabort(self):
    return self('abortabortabort', use_post_method=True)
