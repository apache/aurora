from collections import defaultdict
from datetime import datetime
import os
import time

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import (
    Label,
    LambdaGauge,
    CompoundMetrics,
    MutatorGauge,
    Observable)
from twitter.common.metrics.metrics import Metrics
from twitter.common.metrics.sampler import DiskMetricReader
from twitter.common.python.dirwrapper import PythonDirectoryWrapper
from twitter.common.python.pex import PexInfo
from twitter.common.quantity import Amount, Time
from twitter.common.string.scanf import ScanfParser
from twitter.mesos.executor.executor_detector import ExecutorDetector

from gen.twitter.thermos.ttypes import TaskState

import psutil


class ExecutorVars(Observable):
  """
    Per-executor exported vars.
  """
  ALL_METRICS = ('rss', 'cpu', 'thermos_pss', 'thermos_cpu', 'version')
  RELEASE_TAG_FORMAT = ScanfParser('%(project)s_R%(release)d')
  DEPLOY_TAG_FORMAT = ScanfParser('%(project)s_%(environment)s_%(release)d_R%(deploy)d')
  PROJECT_NAMES = ('thermos', 'thermos_executor')

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

  def __init__(self, process):
    self.orphan = False
    self._process = process

    # manage internal metrics
    self._metrics = dict((metric, MutatorGauge(metric, 0)) for metric in self.ALL_METRICS)
    self._internal_metrics = Metrics()
    for metric in self._metrics.values():
      self._internal_metrics.register(metric)
    self._executor_cwd = process.getcwd()
    self.version = self.get_release_from_binary(os.path.join(self._executor_cwd,
         process.cmdline[1]))
    self._internal_metrics.register(Label('version', self.version))
    self._internal_metrics.register(LambdaGauge('orphan', lambda: int(self.orphan)))

    # manage external metrics
    self._external_metrics = DiskMetricReader(
        os.path.join(self._executor_cwd, ExecutorDetector.VARS_PATH))
    self._external_metrics.start()

    # combine the two
    self._compound_metrics = CompoundMetrics(self._internal_metrics, self._external_metrics)

  @property
  def metrics(self):
    return self._compound_metrics

  def write_metric(self, metric, value):
    self._metrics[metric].write(value)

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
      self.write_metric('cpu', executor_cpu)
      self.write_metric('rss', executor_rss)
      self.orphan = self._process.ppid == 1
    except psutil.error.Error:
      return False

    try:
      child_stats = map(self.cpu_rss_pss, self.thermos_children(self._process))
      self.write_metric('thermos_cpu', sum(stat[0] for stat in child_stats))
      self.write_metric('thermos_pss', sum(stat[2] for stat in child_stats))
    except psutil.error.Error:
      pass

    return True

  def stop(self):
    self._external_metrics.stop()


class MesosObserverVars(Observable, ExceptionalThread):
  COLLECTION_INTERVAL = Amount(30, Time.SECONDS)
  EXECUTOR_BINARIES = ('./thermos_executor', './thermos_executor.pex')
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
    self._executor_metrics = self.metrics.scope('executor')
    self._executors = {}
    self._executor_releases = {}
    self._self = psutil.Process(os.getpid())
    for stat in self.OBSERVER_STATS:
      setattr(self, stat, self.metrics.register(MutatorGauge(stat, 0)))
    super(MesosObserverVars, self).__init__()
    self.daemon = True

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
      if executor.executor_id not in self._executors:
        try:
          executor_vars = ExecutorVars(proc)
        except psutil.error.Error:
          continue
        self._executors[executor.executor_id] = executor_vars
        if not self.executor_filter(executor):
          self._executor_metrics.register_observable(executor.executor_id, executor_vars)
    purge = set()
    for eid, executor in self._executors.items():
      if not executor.sample():
        executor.stop()
        self._executor_metrics.unregister_observable(eid)
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
