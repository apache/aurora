import mimetypes
import os

from twitter.common import log
from twitter.common.http.server import HttpServer

from bottle import HTTPResponse
import pkg_resources


class StaticAssets(object):
  """
    Serve the /assets directory.
  """
  def __init__(self):
    self._assets = {}
    self._detect_assets()

  def _detect_assets(self):
    log.info('detecting assets...')
    assets = pkg_resources.resource_listdir(__name__, 'assets')
    cached_assets = {}
    for asset in assets:
      log.info('  detected asset: %s' % asset)
      cached_assets[asset] = pkg_resources.resource_string(
        __name__, os.path.join('assets', asset))
    self._assets = cached_assets

  @HttpServer.route("/favicon.ico")
  def handle_favicon(self):
    HttpServer.redirect("/assets/favicon.ico")

  @HttpServer.route("/assets/:filename")
  def handle_asset(self, filename):
    # TODO(wickman)  Add static_content to bottle.
    if filename in self._assets:
      mimetype, encoding = mimetypes.guess_type(filename)
      headers = {}
      if mimetype: headers['Content-Type'] = mimetype
      if encoding: headers['Content-Encoding'] = encoding
      return HTTPResponse(self._assets[filename], header=headers)
    else:
      HttpServer.abort(404, 'Unknown asset: %s' % filename)
