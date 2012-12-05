from datetime import datetime
import socket
import sys
import threading
import time

from twitter.common import app, log
from twitter.common.app.modules.http import RootServer
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import MutatorGauge, RootMetrics
from twitter.common.quantity import Amount, Time
from twitter.thermos.base.path import TaskPath
from twitter.thermos.observer.observer import TaskObserver
from twitter.thermos.observer.http import BottleObserver

from gen.twitter.thermos.ttypes import TaskState

import psutil


app.add_option("--root",
               dest="root",
               metavar="DIR",
               default=TaskPath.DEFAULT_CHECKPOINT_ROOT,
               help="root checkpoint directory for thermos task runners")


app.configure(module='twitter.common.app.modules.http',
    port=1338, host=socket.gethostname(), enable=True, framework='cherrypy')
app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_observer')


class MesosObserverVars(ExceptionalThread):
  COLLECTION_INTERVAL = Amount(30, Time.SECONDS)
  METRIC_NAMESPACE = 'observer'
  EXECUTOR_BINARY = './thermos_executor.pex'
  STATUS_MAP = dict((getattr(psutil, status_name), status_name) for status_name in dir(psutil)
                    if status_name.startswith('STATUS_'))

  def __init__(self, observer):
    self._observer = observer
    self.initialize_stats()
    super(MesosObserverVars, self).__init__()
    self.daemon = True

  def metrics(self):
    return RootMetrics().scope(self.METRIC_NAMESPACE)

  def initialize_stats(self):
    metrics = self.metrics()
    self.orphaned_runners = metrics.register(MutatorGauge('orphaned_runners', 0))
    self.orphaned_tasks = metrics.register(MutatorGauge('orphaned_tasks', 0))
    self.orphaned_executors = metrics.register(MutatorGauge('orphaned_executors', 0))
    self.active_tasks = metrics.register(MutatorGauge('active_tasks', 0))
    self.finished_tasks = metrics.register(MutatorGauge('finished_tasks', 0))

  def collect(self):
    self.collect_orphans()
    self.collect_actives()
    self.collect_unparented_executors()

  def collect_actives(self):
    counts = self._observer.task_id_count()
    self.active_tasks.write(counts['active'])
    self.finished_tasks.write(counts['finished'])

  def collect_unparented_executors(self):
    def is_orphan_executor(proc):
      try:
        if proc.ppid == 1 and len(proc.cmdline) >= 2:
          if proc.cmdline[0].startswith('python') and proc.cmdline[1] == self.EXECUTOR_BINARY:
            log.debug('Orphaned executor: pid=%d status=%s cwd=%s age=%s' % (
                 proc.pid,
                 self.STATUS_MAP.get(proc.status, 'UNKNOWN'),
                 proc.getcwd(),
                 datetime.now() - datetime.fromtimestamp(proc.create_time)))
            return True
      except psutil.error.Error as e:
        log.error('Failed to collect stats for %s: %s' % (proc, e))
    self.orphaned_executors.write(len(filter(is_orphan_executor, psutil.process_iter())))

  def collect_orphans(self):
    orphaned_tasks, orphaned_runners = 0, 0
    task_states = (self._observer.raw_state(task_id) for task_id in
        self._observer.task_ids(type='active', num=sys.maxint)['task_ids'])
    for task in filter(None, task_states):
      if not task.header or not task.statuses or len(task.statuses) == 0:
        continue
      status = task.statuses[-1]
      if status.state not in (TaskState.ACTIVE, TaskState.CLEANING, TaskState.FINALIZING):
        continue
      try:
        runner = psutil.Process(status.runner_pid)
      except psutil.NoSuchProcess:
        orphaned_tasks += 1
        continue
      try:
        executor = psutil.Process(runner.ppid)
      except psutil.NoSuchProcess:
        orphaned_runners += 1
        continue
      # reparented by init
      if executor.pid == 1:
        orphaned_runners += 1
    self.orphaned_runners.write(orphaned_runners)
    self.orphaned_tasks.write(orphaned_tasks)

  def run(self):
    while True:
      start = time.time()
      self.collect()
      collection_time = time.time() - start
      log.debug('Metrics collection took %2.1fms' % (1000.0 * collection_time))
      time.sleep(max(0, self.COLLECTION_INTERVAL.as_(Time.SECONDS) - collection_time))


def main(_, opts):
  task_observer = TaskObserver(opts.root)
  task_observer.start()

  observer_vars = MesosObserverVars(task_observer)
  observer_vars.start()

  bottle_wrapper = BottleObserver(task_observer)
  RootServer().mount_routes(bottle_wrapper)

  while True:
    time.sleep(10)


app.main()
