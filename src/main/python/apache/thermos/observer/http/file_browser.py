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

import os
import pprint
from xml.sax.saxutils import escape

import bottle
from mako.template import Template
from twitter.common import log
from twitter.common.http import HttpServer

from .templating import HttpTemplate

MB = 1024 * 1024
DEFAULT_CHUNK_LENGTH = MB
MAX_CHUNK_LENGTH = 16 * MB


def _read_chunk(filename, offset=None, length=None):
  offset = offset or -1
  length = length or -1

  try:
    length = long(length)
    offset = long(offset)
  except ValueError:
    return {}

  if not os.path.isfile(filename):
    return {}

  try:
    fstat = os.stat(filename)
  except Exception as e:
    log.error('Could not read from %s: %s' % (filename, e))
    return {}

  if offset == -1:
    offset = fstat.st_size

  if length == -1:
    length = fstat.st_size - offset

  with open(filename, "r") as fp:
    fp.seek(offset)
    try:
      data = fp.read(length)
    except IOError as e:
      log.error('Failed to read %s: %s' % (filename, e), exc_info=True)
      return {}

  if data:
    return dict(offset=offset, length=len(data), data=escape(data.decode('utf8', 'replace')))

  return dict(offset=offset, length=0)


class TaskObserverFileBrowser(object):
  """
    Mixin for Thermos observer File browser.
  """

  @HttpServer.route("/logs/:task_id/:process/:run/:logtype")
  @HttpServer.mako_view(HttpTemplate.load('logbrowse'))
  def handle_logs(self, task_id, process, run, logtype):
    types = self._observer.logs(task_id, process, int(run))
    if logtype not in types:
      bottle.abort(404, "No such log type: %s" % logtype)
    base, path = types[logtype]
    filename = os.path.join(base, path)
    return {
      'task_id': task_id,
      'filename': filename,
      'process': process,
      'run': run,
      'logtype': logtype
    }

  @HttpServer.route("/logdata/:task_id/:process/:run/:logtype")
  def handle_logdata(self, task_id, process, run, logtype):
    offset = self.Request.GET.get('offset', -1)
    length = self.Request.GET.get('length', -1)
    types = self._observer.logs(task_id, process, int(run))
    if logtype not in types:
      return {}
    chroot, path = types[logtype]
    return _read_chunk(os.path.join(chroot, path), offset, length)

  @HttpServer.route("/file/:task_id/:path#.+#")
  @HttpServer.mako_view(HttpTemplate.load('filebrowse'))
  def handle_file(self, task_id, path):
    if path is None:
      bottle.abort(404, "No such file")
    return {
      'task_id': task_id,
      'filename': path,
    }

  @HttpServer.route("/filedata/:task_id/:path#.+#")
  def handle_filedata(self, task_id, path):
    if path is None:
      return {}
    offset = self.Request.GET.get('offset', -1)
    length = self.Request.GET.get('length', -1)
    chroot, path = self._observer.valid_file(task_id, path)
    if chroot is None or path is None:
      return {}
    return _read_chunk(os.path.join(chroot, path), offset, length)

  @HttpServer.route("/browse/:task_id")
  @HttpServer.route("/browse/:task_id/:path#.*#")
  @HttpServer.mako_view(HttpTemplate.load('filelist'))
  def handle_dir(self, task_id, path=None):
    if path == "":
      path = None
    chroot, path = self._observer.valid_path(task_id, path)
    return dict(task_id=task_id, chroot=chroot, path=path)

  @HttpServer.route("/download/:task_id/:path#.+#")
  def handle_download(self, task_id, path=None):
    chroot, path = self._observer.valid_path(task_id, path)
    if path is None:
      bottle.abort(404, "No such file")
    return bottle.static_file(path, root=chroot, download=True)
