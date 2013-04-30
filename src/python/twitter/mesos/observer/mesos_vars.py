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


class ExecutorVars(object):
  """
    Per-executor exported vars.
  """

  ALL_METRICS = ('executor_rss', 'executor_cpu', 'thermos_pss', 'thermos_cpu', 'version')
  RELEASE_TAG_FORMAT = ScanfParser('%(project)s_R%(release)d')
  DEPLOY_TAG_FORMAT = ScanfParser('%(project)s_%(environment)s_%(release)d_R%(deploy)d')
  PROJECT_NAMES = ('thermos', 'thermos_executor')

  @classmethod
  def metric_name(cls, executor, metric):
    return 'executor_%s_%s' % (executor, metric)

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
  def get_release_from_binary(cls, binary):
    try:
      pex_info = PexInfo.from_pex(PythonDirectoryWrapper.get(binary))
      return cls.get_release_from_tag(pex_info.build_properties.get('tag', ''))
    except PythonDirectoryWrapper.Error:
      return 'UNKNOWN'

  def __init__(self, scope, executor, process):
    self._scope = scope
    self._metrics = dict((metric, MutatorGauge(self.metric_name(executor, metric), 0))
        for metric in self.ALL_METRICS)
    for metric in self._metrics.values():
      scope.register(metric)
    self._process = process
    self.version = self.get_release_from_binary(
        os.path.join(process.getcwd(), process.cmdline[1]))
    self._metrics['version'].write(self.version)
    self.orphan = False

  def unregister(self):
    for metric in self._metrics.values():
      self._scope.unregister(metric.name())

  @classmethod
  def thermos_children(cls, parent):
    try:
      for child in parent.get_children():
        yield child  # thermos_runner
        try:
          for grandchild in child.get_children():
            yield grandchild  # thermos_coordinator
        except psutil.error.Error:
          continue
    except psutil.error.Error:
      return

  @classmethod
  def aggregate_memory(cls, process, attribute='pss'):
    try:
      return sum(getattr(mmap, attribute) for mmap in process.get_memory_maps())
    except psutil.error.Error:
      return 0

  @classmethod
  def cpu_rss_pss(cls, process):
    return (process.get_cpu_percent(0),
            process.get_memory_info().rss,
            cls.aggregate_memory(process, attribute='pss'))

  def sample(self):
    try:
      executor_cpu, executor_rss, _ = self.cpu_rss_pss(self._process)
      self._metrics['executor_cpu'].write(executor_cpu)
      self._metrics['executor_rss'].write(executor_rss)
      self.orphan = self._process.ppid == 1
    except psutil.error.Error:
      return False

    try:
      child_stats = map(self.cpu_rss_pss, self.thermos_children(self._process))
      self._metrics['thermos_cpu'].write(sum(stat[0] for stat in child_stats))
      self._metrics['thermos_pss'].write(sum(stat[2] for stat in child_stats))
    except psutil.error.Error:
      pass

    return True


class MesosObserverVars(ExceptionalThread):
  COLLECTION_INTERVAL = Amount(30, Time.SECONDS)
  METRIC_NAMESPACE = 'observer'
  EXECUTOR_BINARIES = ('./thermos_executor', './thermos_executor.pex')
  STATUS_MAP = dict((getattr(psutil, status_name), status_name) for status_name in dir(psutil)
                    if status_name.startswith('STATUS_'))

  OBSERVER_STATS = ('orphaned_runners',
                    'orphaned_tasks',
                    'orphaned_executors',
                    'active_tasks',
                    'finished_tasks',
                    'observer_rss',
                    'observer_cpu')

  def __init__(self, observer, mesos_root):
    self._mesos_root = mesos_root
    self._detector = ExecutorDetector()
    self._observer = observer
    self._executors = {}  # executor_id -> ExecutorVars
    self._executor_releases = {}
    self._self = psutil.Process(os.getpid())
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
    self.collect_executors()
    self.collect_observer_resource_usage()

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
        log.debug('Orphaned process (missing runner): %s' % task.header.task_id)
        orphaned_tasks += 1
        continue
      try:
        executor = psutil.Process(runner.ppid)
      except psutil.NoSuchProcess:
        log.debug('Orphaned runner (missing executor): %s' % task.header.task_id)
        orphaned_runners += 1
        continue
      # reparented by init
      if executor.pid == 1:
        log.debug('Orphaned runner (parented by init): %s' % task.header.task_id)
        orphaned_runners += 1
    self.orphaned_runners.write(orphaned_runners)
    self.orphaned_tasks.write(orphaned_tasks)

  def collect_tasks(self):
    counts = self._observer.task_id_count()
    self.active_tasks.write(counts['active'])
    self.finished_tasks.write(counts['finished'])

  @classmethod
  def is_executor(cls, proc):
    try:
      return len(proc.cmdline) >= 2 and proc.cmdline[0].startswith('python') and (
          proc.cmdline[1] in cls.EXECUTOR_BINARIES)
    except psutil.error.Error:
      return False

  @classmethod
  def iter_executors(cls):
    try:
      for proc in filter(cls.is_executor, psutil.process_iter()):
        yield proc
    except psutil.error.Error as e:
      log.error("Failed to iterate over processes: %s" % e)

  @classmethod
  def executor_filter(cls, executor):
    """Return true if an executor should be filtered.  Provides a way of
       excluding certain executors from being monitored."""
    return False

  def collect_executors(self):
    self.sample_executors()
    self.index_executors()

  def sample_executors(self):
    for proc in self.iter_executors():
      try:
        cwd = proc.getcwd()
      except psutil.error.Error as e:
        continue
      executor = self._detector.match(cwd)
      if not executor:
        log.warning('Found an executor not running in expected sandbox: %s' % cwd)
        continue
      if self.executor_filter(executor):
        continue
      if executor.executor_id not in self._executors:
        try:
          self._executors[executor.executor_id] = ExecutorVars(self.metrics,
              executor.executor_id, proc)
        except psutil.error.Error:
          continue
    purge = set()
    for eid, executor in self._executors.items():
      if not executor.sample():
        executor.unregister()
        purge.add(eid)
    for eid in purge:
      self._executors.pop(eid)

  def index_executors(self):
    self.orphaned_executors.write(
      sum(executor.orphan for executor in self._executors.values()))

    releases_count = defaultdict(int)
    for executor in self._executors.values():
      releases_count[executor.version] += 1

    for release, count in releases_count.items():
      if release not in self._executor_releases:
        gauge = MutatorGauge('executor_version_%s' % release)
        self._executor_releases[release] = gauge
        self.metrics.register(gauge)
      self._executor_releases[release].write(count)

    for old_release in set(self._executor_releases) - set(releases_count):
      gauge = self._executor_releases.pop(old_release)
      self.metrics.unregister(gauge.name())

  def collect_observer_resource_usage(self):
    self.observer_cpu.write(self._self.get_cpu_percent(interval=0))
    self.observer_rss.write(self._self.get_memory_info().rss)

  def run(self):
    while True:
      start = time.time()
      self.collect()
      collection_time = time.time() - start
      log.debug('Metrics collection took %2.1fms' % (1000.0 * collection_time))
      time.sleep(max(0, self.COLLECTION_INTERVAL.as_(Time.SECONDS) - collection_time))
