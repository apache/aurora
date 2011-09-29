import os
import mimetypes
import pkg_resources

from twitter.common import log

from server import BottleServer, BottleTemplate

# mixins
# when things settle, discover mixins instead of static
# declaration.
from file_browser import TaskObserverFileBrowser
from .json import TaskObserverJSONBindings
BottleObserverMixins = [
  TaskObserverFileBrowser,
  TaskObserverJSONBindings
]

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

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

  @BottleServer.route("/favicon.ico")
  def handle_favicon(self):
    self.bottle.redirect("/assets/favicon.ico")

  @BottleServer.route("/assets/:filename")
  def handle_asset(self, filename):
    # TODO(wickman)  Add static_content to bottle.
    if filename in self._assets:
      mimetype, encoding = mimetypes.guess_type(filename)
      header = {}
      if mimetype: header['Content-Type'] = mimetype
      if encoding: header['Content-Encoding'] = encoding
      return BottleServer.Response(self._assets[filename], header=header)
    else:
      return BottleServer.Response(status=404)

def _flatten(lists):
  out = []
  for item in lists:
    if isinstance(item, (list, tuple)):
      out.extend(_flatten(item))
    else:
      out.append(item)
  return out

class ListExpansionMetaclass(type):
  def __new__(mcls, name, parents, attrs):
    parents = _flatten(parents)
    return type(name, tuple(parents), attrs)

class BottleObserver(BottleServer, StaticAssets, BottleObserverMixins):
  """
    A bottle wrapper around a Thermos TaskObserver.
  """

  # Because Python doesn't like *list syntax in class declarations.
  __metaclass__ = ListExpansionMetaclass

  def __init__(self, observer):
    self._observer = observer
    # Can these be auto-grokked?
    StaticAssets.__init__(self)
    for mixin in BottleObserverMixins:
      mixin.__init__(self)
    BottleServer.__init__(self)

  @BottleServer.route("/")
  @BottleServer.view(BottleTemplate.load('index'))
  def handle_index(self):
    return dict(
      hostname = self.hostname()
    )
