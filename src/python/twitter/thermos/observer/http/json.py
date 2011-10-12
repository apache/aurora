import urllib
from twitter.common.http import HttpServer

class TaskObserverJSONBindings(object):
  """
    Mixin for Thermos observer JSON endpoints.
  """

  @HttpServer.route("/j/uids")
  @HttpServer.route("/j/uids/:which")
  @HttpServer.route("/j/uids/:which/:offset")
  @HttpServer.route("/j/uids/:which/:offset/:num")
  def handle_uids(self, which=None, offset=None, num=None):
    return self._observer.uids(
      which,
      int(offset) if offset is not None else 0,
      int(num) if num is not None else 20)

  @HttpServer.route("/j/uid_count")
  def handle_uid_count(self):
    return self._observer.uid_count()

  @HttpServer.route("/j/task")
  def handle_tasks(self):
    """
      Additional parameters:
        uid = comma separated list of uids.
    """
    uids = HttpServer.Request.GET.get('uid', [])
    if uids:
      uids = urllib.unquote(uids).split(',')
    return self._observer.task(uids)

  @HttpServer.route("/j/task/:uid")
  def handle_task(self, uid):
    return self._observer.task([uid])

  @HttpServer.route("/j/process/:uid")
  @HttpServer.route("/j/process/:uid/:process")
  @HttpServer.route("/j/process/:uid/:process/:run")
  def handle_process(self, uid, process=None, run=None):
    return self._observer.process(uid, process, run)

  @HttpServer.route("/j/processes")
  def handle_processes(self):
    """
      Additional parameters:
        uids = comma separated list of uids.
    """
    uids = HttpServer.Request.GET.get('uid', [])
    if uids:
      uids = urllib.unquote(uids).split(',')
    return self._observer.processes(uids)

