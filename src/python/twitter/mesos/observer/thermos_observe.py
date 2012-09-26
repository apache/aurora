from __future__ import print_function

import socket
import sys
import threading
import time

from twitter.common import app, log
from twitter.common.app.modules.http import RootServer
from twitter.common.metrics import MutatorGauge, RootMetrics
from twitter.common.quantity import Amount, Time
from twitter.thermos.observer.observer import TaskObserver
from twitter.thermos.observer.http import BottleObserver

from gen.twitter.thermos.ttypes import TaskState

import psutil


app.add_option("--root",
               dest="root",
               metavar="DIR",
               default="/var/run/thermos",
               help="root checkpoint directory for thermos task runners")


app.configure(module='twitter.common.app.modules.http',
    port=1338, host=socket.gethostname(), enable=True, framework='tornado')
app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_observer')


class MesosObserverVars(threading.Thread):
  COLLECTION_INTERVAL = Amount(15, Time.SECONDS)
  METRIC_NAMESPACE = 'observer'
  THERMOS_EXECUTOR_BINARY = './thermos_executor.pex'

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
    self.active_tasks = metrics.register(MutatorGauge('active_tasks', 0))
    self.finished_tasks = metrics.register(MutatorGauge('finished_tasks', 0))

  def collect(self):
    self.collect_orphans()
    self.collect_actives()

  def collect_actives(self):
    counts = self._observer.task_id_count()
    self.active_tasks.write(counts['active'])
    self.finished_tasks.write(counts['finished'])

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
        log.debug('Detected orphaned task: %s' % task.header.task_id)
        orphaned_tasks += 1
        continue
      try:
        executor = psutil.Process(runner.ppid)
      except psutil.NoSuchProcess:
        log.debug('Detected orphaned runner: %s' % task.header.task_id)
        orphaned_runners += 1
        continue
      # reparented by init
      if executor.pid == 1:
        log.debug('Detected orphaned runner: %s' % task.header.task_id)
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
