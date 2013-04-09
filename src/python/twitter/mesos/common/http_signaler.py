import contextlib
from socket import timeout as SocketTimeout
import socks
import sys

from twitter.common import log
from twitter.common.lang import Compatibility
from twitter.common.net.tunnel import TunnelHelper
from twitter.common_internal.location import Location
from twitter.mesos.clusters import Cluster

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

  @classmethod
  def maybe_setup_proxy(cls, cluster):
    if Location.is_corp() and not Cluster.get(cluster).force_notunnel:
      log.info('Setting up SOCKS proxy for http health checks.')
      proxy_host, proxy_port = TunnelHelper.create_proxy()
      socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, proxy_host, proxy_port)
      socks.wrapmodule(urllib_request)

  def __init__(self, port, host='localhost', timeout_secs=TIMEOUT_SECS):
    self._url_base = 'http://%s:%d/' % (host, port)
    self._timeout_secs = timeout_secs

  def url(self, endpoint):
    return self._url_base + endpoint

  def __call__(self, endpoint, use_post_method, expected_response=None):
    """Returns a (boolean, string|None) tuple of (call success, failure reason)"""
    try:
      data = '' if use_post_method else None
      with contextlib.closing(
          urllib_request.urlopen(self.url(endpoint), data, timeout=self._timeout_secs)) as fp:
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
    except (URLError, HTTPError, HTTPException, SocketTimeout, socks.GeneralProxyError) as e:
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
