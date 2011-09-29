import urllib
from server import BottleServer

class TaskObserverJSONBindings(object):
  """
    Mixin for Thermos observer JSON endpoints.
  """

  @BottleServer.route("/uids")
  @BottleServer.route("/uids/:which")
  @BottleServer.route("/uids/:which/:offset")
  @BottleServer.route("/uids/:which/:offset/:num")
  def handle_uids(self, which=None, offset=None, num=None):
    return self._observer.uids(
      which,
      int(offset) if offset is not None else 0,
      int(num) if num is not None else 20)

  @BottleServer.route("/uid_count")
  def handle_uid_count(self):
    return self._observer.uid_count()

  @BottleServer.route("/task")
  def handle_tasks(self):
    """
      Additional parameters:
        uid = comma separated list of uids.
    """
    uids = self._request.GET.get('uid', [])
    if uids:
      uids = urllib.unquote(uids).split(',')
    return self._observer.task(uids)

  @BottleServer.route("/task/:uid")
  def handle_task(self, uid):
    return self._observer.task([uid])

  @BottleServer.route("/process/:uid")
  @BottleServer.route("/process/:uid/:process")
  @BottleServer.route("/process/:uid/:process/:run")
  def handle_process(self, uid, process=None, run=None):
    return self._observer.process(uid, process, run)

  @BottleServer.route("/processes")
  def handle_processes(self):
    """
      Additional parameters:
        uids = comma separated list of uids.
    """
    uids = self._request.GET.get('uid', [])
    if uids:
      uids = urllib.unquote(uids).split(',')
    return self._observer.processes(uids)

