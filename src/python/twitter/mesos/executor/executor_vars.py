import os

from twitter.common.metrics import LambdaGauge, MutatorGauge, Observable
from twitter.common.python.dirwrapper import PythonDirectoryWrapper
from twitter.common.python.pex import PexInfo
from twitter.common.quantity import Amount, Time
from twitter.common.string.scanf import ScanfParser

import psutil


class ExecutorVars(Observable):
  """
    Executor exported /vars wrapper.

    Currently writes to disk and communicates through the Aurora Observer,
    pending MESOS-433.
  """
  MUTATOR_METRICS = ('rss', 'cpu', 'thermos_pss', 'thermos_cpu')
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

  def __init__(self):
    self.metrics.register(NamedGauge('version', self.get_release_from_binary(
        os.path.join(process.getcwd(), process.cmdline[1]))))
    self._self = psutil.Process(os.getpid())
    self._orphan = False
    self.metrics.register(LambdaGauge('orphan', lambda: self._orphan))
    self._metrics = dict((metric, MutatorGauge(metric, 0)) for metric in self.MUTATOR_METRICS)
    for metric in self._metrics.values():
      self.metrics.register(metric)

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
      executor_cpu, executor_rss, _ = self.cpu_rss_pss(self._self)
      self.write_metric('cpu', executor_cpu)
      self.write_metric('rss', executor_rss)
      self._orphan = self._self.ppid == 1
    except psutil.error.Error:
      return False

    try:
      child_stats = map(self.cpu_rss_pss, self.thermos_children(self._self))
      self.write_metric('thermos_cpu', sum(stat[0] for stat in child_stats))
      self.write_metric('thermos_pss', sum(stat[2] for stat in child_stats))
    except psutil.error.Error:
      pass

    return True
