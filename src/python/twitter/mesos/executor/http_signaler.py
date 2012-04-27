import contextlib
from twitter.common.lang import Compatibility

if Compatibility.PY3:
  from urllib.request import urlopen
  from urllib.error import URLError
else:
  from urllib2 import urlopen, URLError


class HttpSignaler(object):
  TIMEOUT_SECS = 1.0

  def __init__(self, port, host='localhost'):
    self._url_base = 'http://%s:%d/' % (host, port)

  def url(self, endpoint):
    return self._url_base + endpoint

  def __call__(self, endpoint, expected_response=None):
    try:
      with contextlib.closing(urlopen(self.url(endpoint), timeout=self.TIMEOUT_SECS)) as fp:
        response = fp.read().strip().lower()
        return response == expected_response if expected_response is not None else True
    except URLError as e:
      return False

  def health(self):
    return self('health', 'ok')

  def quitquitquit(self):
    return self('quitquitquit')

  def abortabortabort(self):
    return self('abortabortabort')
