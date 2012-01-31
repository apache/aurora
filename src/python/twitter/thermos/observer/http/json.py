import urllib
from twitter.common.http import HttpServer

class TaskObserverJSONBindings(object):
  """
    Mixin for Thermos observer JSON endpoints.
  """

  @HttpServer.route("/j/task_ids")
  @HttpServer.route("/j/task_ids/:which")
  @HttpServer.route("/j/task_ids/:which/:offset")
  @HttpServer.route("/j/task_ids/:which/:offset/:num")
  def handle_task_ids(self, which=None, offset=None, num=None):
    return self._observer.task_ids(
      which,
      int(offset) if offset is not None else 0,
      int(num) if num is not None else 20)

  @HttpServer.route("/j/task_id_count")
  def handle_task_id_count(self):
    return self._observer.task_id_count()

  @HttpServer.route("/j/task")
  def handle_tasks(self):
    """
      Additional parameters:
        task_id = comma separated list of task_ids.
    """
    task_ids = HttpServer.Request.GET.get('task_id', [])
    if task_ids:
      task_ids = urllib.unquote(task_ids).split(',')
    return self._observer.task(task_ids)

  @HttpServer.route("/j/task/:task_id")
  def handle_task(self, task_id):
    return self._observer.task([task_id])

  @HttpServer.route("/j/process/:task_id")
  @HttpServer.route("/j/process/:task_id/:process")
  @HttpServer.route("/j/process/:task_id/:process/:run")
  def handle_process(self, task_id, process=None, run=None):
    return self._observer.process(task_id, process, run)

  @HttpServer.route("/j/processes")
  def handle_processes(self):
    """
      Additional parameters:
        task_ids = comma separated list of task_ids.
    """
    task_ids = HttpServer.Request.GET.get('task_id', [])
    if task_ids:
      task_ids = urllib.unquote(task_ids).split(',')
    return self._observer.processes(task_ids)

