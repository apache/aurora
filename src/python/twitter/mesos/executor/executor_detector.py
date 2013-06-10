import os
from glob import glob

from twitter.common.string import ScanfParser
from twitter.mesos.common_internal.clusters import TwitterCluster


class ExecutorDetector(object):
  LOG_PATH = 'executor_logs'
  RESOURCE_PATH = 'resource_usage.recordio'
  VARS_PATH = 'executor_vars.json'

  PATTERN = [
      '%(root)s',
      'slaves',
      '%(slave_id)s',
      'frameworks',
      '%(framework_id)s',
      'executors',
      '%(executor_id)s',
      'runs',
      '%(run)s']

  def __init__(self):
    self._extractor = ScanfParser(os.path.join(*self.PATTERN))

  def __iter__(self):
    for extraction in self.find():
      yield extraction

  def match(self, path):
    try:
      return self._extractor.parse(path)
    except ScanfParser.ParseError:
      return None

  def path(self, result):
    return os.path.join(*self.PATTERN) % result.groups()

  def find(self, root=TwitterCluster.DEFAULT_MESOS_ROOT,
           slave_id='*', framework_id='*', executor_id='*', run='*'):
    mixins = dict(
        root=root, slave_id=slave_id, framework_id=framework_id, executor_id=executor_id, run=run)
    return filter(None, map(self.match, glob(os.path.join(*self.PATTERN) % mixins)))
