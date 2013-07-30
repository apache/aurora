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

  EXTRACTOR = ScanfParser(os.path.join(*PATTERN))

  @classmethod
  def __iter__(cls):
    for extraction in cls.find():
      yield extraction

  @classmethod
  def match(cls, path):
    try:
      return cls._extractor.parse(path)
    except ScanfParser.ParseError:
      return None

  @classmethod
  def path(cls, result):
    return os.path.join(*cls.PATTERN) % result.groups()

  @classmethod
  def find(cls, root=TwitterCluster.DEFAULT_MESOS_ROOT,
           slave_id='*', framework_id='*', executor_id='*', run='*'):
    mixins = dict(
        root=root, slave_id=slave_id, framework_id=framework_id, executor_id=executor_id, run=run)
    return filter(None, map(cls.match, glob(os.path.join(*cls.PATTERN) % mixins)))
