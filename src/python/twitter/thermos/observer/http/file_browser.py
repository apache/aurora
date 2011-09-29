import os
import bottle
from server import BottleServer, BottleTemplate

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
  if bytes is None or bytes < 0:
    bytes = DEFAULT_CHUNK_LENGTH
  if bytes > MAX_CHUNK_LENGTH:
    bytes = MAX_CHUNK_LENGTH
  if offset is None:
    offset = filelen - bytes
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

  @BottleServer.route("/logs/:uid/:process/:run")
  def handle_logs(self, uid, process, run):
    """
      Additional parameters:
        path=<path within chroot>
        fmt=<json|html>
    """
    path = self._request.GET.get('path', None)
    format = self._request.GET.get('fmt', 'html')
    return self._observer.logs(uid, process, int(run), path, format)

  @BottleServer.route("/file/:uid/:path#.+#")
  @BottleServer.mako_view(BottleTemplate.load('filebrowse'))
  def handle_file(self, uid, path):
    """
      Additional parameters:
        offset= (default 0)
        bytes=  (default 1048576)
    """
    if path is None:
      bottle.abort(404, "No such file")

    offset = self._request.GET.get('offset', None)
    bytes = self._request.GET.get('bytes', None)
    chroot, path = self._observer.file_path(uid, path)
    if chroot is None or path is None:
      bottle.abort(404, "Invalid file path")

    d = _read_chunk(os.path.join(chroot, path), offset, bytes)
    if not d:
      bottle.abort(404, "Unable to read file")
    d.update(filename = path, uid = uid)
    print 'Setting d to %s' % repr(d)
    return d

  @BottleServer.route("/browse/:uid")
  @BottleServer.route("/browse/:uid/:path#.+#")
  @BottleServer.mako_view(BottleTemplate.load('filelist'))
  def handle_dir(self, uid, path=None):
    return self._observer.files(uid, path)

  @BottleServer.route("/download/:uid/:path#.+#")
  def handle_download(self, uid, path=None):
    root_uid = self._observer.files(uid)
    return bottle.static_file(path, root = root_uid['chroot'], download=True)
