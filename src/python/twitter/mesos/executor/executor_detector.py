import os
from glob import glob

from twitter.common.string import ScanfParser


class ExecutorDetector(object):
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

  def find(self, root='/var/lib/mesos', slave_id='*', framework_id='*', executor_id='*', run='*'):
    mixins = dict(
        root=root, slave_id=slave_id, framework_id=framework_id, executor_id=executor_id, run=run)
    for path in glob(os.path.join(*self.PATTERN) % mixins):
      try:
        yield self._extractor.parse(path)
      except ScanfParser.ParseError:
        continue
