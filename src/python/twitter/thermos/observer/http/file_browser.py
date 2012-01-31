import os
import bottle
from twitter.common.http import HttpServer
from templating import HttpTemplate

def _read_chunk(filename, offset=None, bytes=None):
  MB = 1024 * 1024
  DEFAULT_CHUNK_LENGTH = MB
  MAX_CHUNK_LENGTH = 16 * MB
  try:
    fstat = os.stat(filename)
    if not os.path.isfile(filename):
      return None
  except:
    return None

  filelen = fstat.st_size
  if bytes is not None:
    try:
      bytes = long(bytes)
    except:
      return None
  if bytes is None or bytes < 0:
    bytes = DEFAULT_CHUNK_LENGTH
  if bytes > MAX_CHUNK_LENGTH:
    bytes = MAX_CHUNK_LENGTH
  if offset is None:
    offset = filelen - bytes
  else:
    try:
      offset = long(offset)
    except:
      return None
  if offset < 0: offset = 0

  with open(filename, "r") as fp:
    fp.seek(offset)
    try:
      data = fp.read(bytes)
      return dict(
        data = data,
        filelen = filelen,
        read = len(data),
        offset = offset,
        bytes = bytes,
        has_more = offset + bytes < filelen
      )
    except:
      return {}

class TaskObserverFileBrowser(object):
  """
    Mixin for Thermos observer File browser.
  """

  @HttpServer.route("/logs/:task_id/:process/:run/:logtype")
  @HttpServer.mako_view(HttpTemplate.load('logbrowse'))
  def handle_logs(self, task_id, process, run, logtype):
    """
      Additional parameters:
        offset= (default 0)
        bytes=  (default 1048576)
    """
    offset = self.Request.GET.get('offset', None)
    bytes = self.Request.GET.get('bytes', None)
    types = self._observer.logs(task_id, process, int(run))
    if logtype not in types:
      bottle.abort(404, "No such log type: %s" % logtype)
    chroot, path = types[logtype]
    data = _read_chunk(os.path.join(chroot, path), offset, bytes)
    if not data:
      bottle.abort(404, "Unable to read %s (%s)" % (logtype, path))
    data.update(
      task_id = task_id,
      process = process,
      run = run,
      logtype = logtype,
    )
    return data

  @HttpServer.route("/file/:task_id/:path#.+#")
  @HttpServer.mako_view(HttpTemplate.load('filebrowse'))
  def handle_file(self, task_id, path):
    """
      Additional parameters:
        offset= (default 0)
        bytes=  (default 1048576)
    """
    if path is None:
      bottle.abort(404, "No such file")

    offset = self.Request.GET.get('offset', None)
    bytes = self.Request.GET.get('bytes', None)
    chroot, path = self._observer.file_path(task_id, path)
    if chroot is None or path is None:
      bottle.abort(404, "Invalid file path")

    d = _read_chunk(os.path.join(chroot, path), offset, bytes)
    if not d:
      bottle.abort(404, "Unable to read file")
    d.update(filename = path, task_id = task_id)
    return d

  @HttpServer.route("/browse/:task_id")
  @HttpServer.route("/browse/:task_id/:path#.+#")
  @HttpServer.mako_view(HttpTemplate.load('filelist'))
  def handle_dir(self, task_id, path=None):
    return self._observer.files(task_id, path)

  @HttpServer.route("/download/:task_id/:path#.+#")
  def handle_download(self, task_id, path=None):
    root_task_id = self._observer.files(task_id)
    return bottle.static_file(path, root = root_task_id['chroot'], download=True)
