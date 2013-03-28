from collections import defaultdict
from datetime import datetime
import os
import time

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import Label, MutatorGauge, RootMetrics
from twitter.common.python.dirwrapper import PythonDirectoryWrapper
from twitter.common.python.pex import PexInfo
from twitter.common.quantity import Amount, Time
from twitter.common.string.scanf import ScanfParser
from twitter.mesos.executor.executor_detector import ExecutorDetector

from gen.twitter.thermos.ttypes import TaskState

import psutil


class MesosObserverVars(ExceptionalThread):
  COLLECTION_INTERVAL = Amount(30, Time.SECONDS)
  METRIC_NAMESPACE = 'observer'
  EXECUTOR_BINARIES = ('./thermos_executor', './thermos_executor.pex')
  STATUS_MAP = dict((getattr(psutil, status_name), status_name) for status_name in dir(psutil)
                    if status_name.startswith('STATUS_'))
  OBSERVER_STATS = ('orphaned_runners', 'orphaned_tasks', 'orphaned_executors',
                    'active_tasks', 'finished_tasks')
  PROJECT_NAMES = ('thermos', 'thermos_executor')
  RELEASE_TAG_FORMAT = ScanfParser('%(project)s_R%(release)d')
  DEPLOY_TAG_FORMAT = ScanfParser('%(project)s_%(environment)s_%(release)d_R%(deploy)d')

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

  @classmethod
  def executor_binaries(cls):
    """Generator yielding (cwd, executor binary) pairs of active executors."""
    try:
      for proc in psutil.process_iter():
        if (len(proc.cmdline) >= 2
            and proc.cmdline[0].startswith('python')
            and proc.cmdline[1] in cls.EXECUTOR_BINARIES):
          yield (proc.getcwd(), proc.cmdline[1])
    except psutil.error.Error as e:
      log.error("Failed to collect information on executors: %s" % e)

  def executor_releases(self):
    """Generator yielding (executor_name, executor_release) for all active executors"""
    active_binaries = dict(self.executor_binaries())
    for executor in self._detector.find(root=self._mesos_root):
      if executor.run == 'latest':
        continue
      path = self._detector.path(executor)
      if path in active_binaries:
        binary = os.path.join(path, active_binaries[path])
        yield (executor.executor_id, self.get_release_from_binary(binary))

  @classmethod
  def get_release_from_tag(cls, tag):
    def parse_from(parser):
      try:
        scanf = parser.parse(tag)
        if scanf and scanf.project in cls.PROJECT_NAMES:
          return scanf.release
      except ScanfParser.ParseError:
        pass
    release = parse_from(cls.RELEASE_TAG_FORMAT)
    if release is None:
      release = parse_from(cls.DEPLOY_TAG_FORMAT)
    if release is None:
      release = 'UNKNOWN'
    return release

  @classmethod
  def get_release_from_binary(cls, binary, memoized={}):
    """Memoized mapping of executor binaries to Thermos release"""
    if binary not in memoized:
      try:
        pex_info = PexInfo.from_pex(PythonDirectoryWrapper.get(binary))
        memoized[binary] = cls.get_release_from_tag(pex_info.build_properties.get('tag', ''))
      except PythonDirectoryWrapper.Error:
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
