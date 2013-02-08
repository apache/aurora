from collections import defaultdict
from datetime import datetime
import os
import socket
import sys
import threading
import time

from twitter.common import app, log
from twitter.common.app.modules.http import RootServer
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import Label, MutatorGauge, RootMetrics
from twitter.common.python.dirwrapper import PythonDirectoryWrapper
from twitter.common.python.pex import PexInfo
from twitter.common.quantity import Amount, Time
from twitter.common.string.scanf import ScanfParser

from twitter.mesos.clusters import Cluster
from twitter.mesos.executor.executor_detector import ExecutorDetector
from twitter.mesos.executor.resource_checkpoints import CheckpointResourceMonitor
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
    port=1338, host='0.0.0.0', enable=True, framework='cherrypy')
app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_observer')


class MesosObserverVars(ExceptionalThread):
  COLLECTION_INTERVAL = Amount(30, Time.SECONDS)
  METRIC_NAMESPACE = 'observer'
  EXECUTOR_BINARY = './thermos_executor.pex'
  STATUS_MAP = dict((getattr(psutil, status_name), status_name) for status_name in dir(psutil)
                    if status_name.startswith('STATUS_'))
  OBSERVER_STATS = ('orphaned_runners', 'orphaned_tasks', 'orphaned_executors',
                    'active_tasks', 'finished_tasks')
  RELEASE_TAG_FORMAT = ScanfParser('%(project)s_%(environment)s_%(release)d_R%(deploy)d')


  def __init__(self, observer, mesos_root):
    self._mesos_root = mesos_root
    self._detector = ExecutorDetector()
    self._observer = observer
    self._executor_releases = {} # release_number => Label(release_number, release_count)
    self.active_executors = self.metrics.register(MutatorGauge('active_executors', {}))
    for stat in self.OBSERVER_STATS:
      setattr(self, stat, self.metrics.register(MutatorGauge(stat, 0)))
    super(MesosObserverVars, self).__init__()
    self.daemon = True

  @property
  def metrics(self):
    return RootMetrics().scope(self.METRIC_NAMESPACE)

  def collect(self):
    self.collect_orphans()
    self.collect_tasks()
    self.collect_running_executors()
    self.collect_unparented_executors()

  def collect_tasks(self):
    counts = self._observer.task_id_count()
    self.active_tasks.write(counts['active'])
    self.finished_tasks.write(counts['finished'])

  def executor_processes(self):
    """Generator yielding psutil.Process objects representing executors currently running"""
    try:
      for proc in psutil.process_iter():
        if (len(proc.cmdline) >= 2
            and proc.cmdline[0].startswith('python')
            and proc.cmdline[1] == self.EXECUTOR_BINARY):
          yield proc
    except psutil.error.Error as e:
      log.error("Failed to collect information on executors: %s" % e)

  def executor_releases(self):
    """Generator yielding (executor_name, executor_release) for all active executors"""
    try:
      active_proc_cwds = [proc.getcwd() for proc in self.executor_processes()]
    except psutil.error.Error as e:
      log.error("Failed to collect information on executors: %s" % e)
    else:
      for executor in self._detector.find(root=self._mesos_root):
        path = self._detector.path(executor)
        if os.path.islink(path):
          continue
        if path in active_proc_cwds:
          binary = os.path.join(path, self.EXECUTOR_BINARY)
          yield (executor.executor_id, self.get_release_from_binary(binary))

  def get_release_from_binary(self, binary, memoized={}):
    """Memoized mapping of executor binaries to Thermos release"""
    if binary not in memoized:
      pex_info = PexInfo.from_pex(PythonDirectoryWrapper.get(binary))
      tag = pex_info.build_properties.get('tag', '')
      try:
        memoized[binary] = self.RELEASE_TAG_FORMAT.parse(tag).release
      except ScanfParser.Error:
        memoized[binary] = 'UNKNOWN'
    return memoized[binary]

  def collect_running_executors(self):
    """Collect stats about running (active) executors"""
    active_exec_releases = dict(self.executor_releases())
    self.active_executors.write(active_exec_releases)
    releases_count = defaultdict(int)
    for release in active_exec_releases.values():
      releases_count[release] += 1

    for release, count in releases_count.items():
      if release not in self._executor_releases:
        gauge = MutatorGauge('executor_version_%s' % release)
        self._executor_releases[release] = gauge
        self.metrics.register(gauge)
      self._executor_releases[release].write(count)

    for old_release in set(self._executor_releases) - set(releases_count):
      self._executor_releases[old_release].write(0)


  def collect_unparented_executors(self):
    """Collect all orphaned executor processes"""
    def is_orphan_executor(proc):
      try:
        if proc.ppid == 1:
          log.debug('Orphaned executor: pid=%d status=%s cwd=%s age=%s' % (
               proc.pid,
               self.STATUS_MAP.get(proc.status, 'UNKNOWN'),
               proc.getcwd(),
               datetime.now() - datetime.fromtimestamp(proc.create_time)))
          return True
      except psutil.error.Error as e:
        log.error('Failed to collect stats for %s: %s' % (proc, e))
    self.orphaned_executors.write(len(filter(is_orphan_executor, self.executor_processes())))

  def collect_orphans(self):
    orphaned_tasks, orphaned_runners = 0, 0
    task_states = (task.state for task_id, task in self._observer.active_tasks.items())
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
  mesos_root = os.environ.get('META_THERMOS_ROOT', Cluster.DEFAULT_MESOS_ROOT)

  # TODO(jon): either fully implement or remove the MesosCheckpointResourceMonitor
  class MesosCheckpointResourceMonitor(CheckpointResourceMonitor):
    MESOS_ROOT = mesos_root

  task_observer = TaskObserver(opts.root)
  task_observer.start()

  observer_vars = MesosObserverVars(task_observer, mesos_root)
  observer_vars.start()

  bottle_wrapper = BottleObserver(task_observer)
  RootServer().mount_routes(bottle_wrapper)

  while True:
    time.sleep(10)


app.main()
