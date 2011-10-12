import os
import mimetypes
import pkg_resources
import socket

from twitter.common import log
from twitter.common.http import HttpServer
from templating import HttpTemplate

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
      header = {}
      if mimetype: header['Content-Type'] = mimetype
      if encoding: header['Content-Encoding'] = encoding
      return HttpServer.Response(self._assets[filename], header=header)
    else:
      return HttpServer.Response(status=404)

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

class BottleObserver(HttpServer, StaticAssets, BottleObserverMixins):
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

  @HttpServer.route("/")
  @HttpServer.view(HttpTemplate.load('index'))
  def handle_index(self):
    return dict(
      hostname = socket.gethostname()
    )

  @HttpServer.route("/task/:uid")
  @HttpServer.mako_view(HttpTemplate.load('task'))
  def handle_task(self, uid):
    task = self._observer.task([uid])
    if not task[uid]:
      return HttpServer.Response(status=404)
    processes = self._observer.processes([uid])
    if not processes[uid]:
      return HttpServer.Response(status=404)

    task = task[uid]
    processes = processes[uid]
    job = task['job']

    return dict(
      hostname = socket.gethostname(),
      uid = uid,
      role = job['role'],
      user = job['user'],
      job = job['name'],
      cluster = job['cluster'],
      replica = task['replica'],
      status = task['state'],
      processes = processes,
      # TODO(wickman)  Fill these in once they become exposed by the
      # runner/executor.
      executor_id = "boosh",
      chroot = "boosh"
    )
