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

import mimetypes
import os

import pkg_resources
from bottle import HTTPResponse
from twitter.common import log
from twitter.common.http.server import HttpServer


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
