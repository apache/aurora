import os
import mimetypes
import pkg_resources
import socket

from twitter.common import log
from twitter.common.http import HttpServer
from templating import HttpTemplate

from .file_browser import TaskObserverFileBrowser
from .json import TaskObserverJSONBindings
BottleObserverMixins = (
  TaskObserverFileBrowser,
  TaskObserverJSONBindings
)

import bottle

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

  @HttpServer.route("/main")
  @HttpServer.route("/main/:type")
  @HttpServer.route("/main/:type/:offset")
  @HttpServer.route("/main/:type/:offset/:num")
  @HttpServer.mako_view(HttpTemplate.load('main'))
  def handle_main(self, type=None, offset=None, num=None):
    if type not in (None, 'all', 'finished', 'active'):
      return HttpServer.Response(status=404)
    if offset is not None:
      try:
        offset = int(offset)
      except:
        return HttpServer.Response(status=404)
    if num is not None:
      try:
        num = int(num)
      except:
        return HttpServer.Response(status=404)
    return self._observer.main(type, offset, num)

  @HttpServer.route("/task/:task_id")
  @HttpServer.mako_view(HttpTemplate.load('task'))
  def handle_task(self, task_id):
    task = self._observer.task([task_id])
    if not task[task_id]:
      bottle.abort(404, "Failed to find task %s.  Try again shortly." % task_id)
    processes = self._observer.processes([task_id])
    if not processes[task_id]:
      return HttpServer.Response(status=404)
    context = self._observer.context(task_id)
    processes = processes[task_id]
    task = task[task_id]
    state = self._observer.state(task_id)

    return dict(
      task_id = task_id,
      statuses = self._observer.task_statuses(task_id),
      user = task['user'],
      ports = task['ports'],
      processes = processes,
      chroot = state.get('sandbox', ''),
      launch_time = state.get('launch_time', 0),
      hostname = state.get('hostname', 'localhost')
    )

  @HttpServer.route("/process/:task_id/:process_id")
  @HttpServer.mako_view(HttpTemplate.load('process'))
  def handle_process(self, task_id, process_id):
    all_processes = {}
    current_run = self._observer.process(task_id, process_id)
    if not current_run:
      return HttpServer.Response(status=404)
    process = self._observer.process_from_name(task_id, process_id)
    if process is None:
      log.error('Could not recover process: %s/%s' % (task_id, process_id))
      return HttpServer.Response(status=404)

    current_run_number = current_run['process_run']
    all_processes[current_run_number] = current_run
    for run in range(current_run_number):
      all_processes[run] = self._observer.process(task_id, process_id, run)
    def convert_process_tuple(run_tuple):
      process_tuple = dict(state = run_tuple['state'])
      if 'start_time' in run_tuple:
        process_tuple.update(start_time = run_tuple['start_time'])
      if 'stop_time' in run_tuple:
        process_tuple.update(stop_time = run_tuple['stop_time'])
      return process_tuple

    template = {
      'task_id': task_id,
      'process': {
         'name': process_id,
         'status': all_processes[current_run_number]["state"],
         'cmdline': process.cmdline().get()
      },
    }
    template['process'].update(**all_processes[current_run_number].get('used', {}))
    template['runs'] = dict((run, convert_process_tuple(run_tuple))
        for run, run_tuple in all_processes.items())
    log.info('Rendering template is: %s' % template)
    return template


