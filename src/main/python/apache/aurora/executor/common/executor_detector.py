#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import glob
import os

from twitter.common.string import ScanfParser


# TODO(wickman) MESOS-2805  This makes an assumption about the directory
# layout Mesos provides.  Ideally we never need to do this and we should
# work with the Mesos core team to make it unnecessary.
class ExecutorDetector(object):
  class Error(Exception): pass
  class CannotFindRoot(Error): pass

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
  def find_root(cls, path):
    """Does this path appear to match the executor directory pattern?"""

    def root_from_path(path):
      path = os.path.normpath(path)
      path_vector = path.split(os.path.sep)
      pattern_vector = cls.PATTERN
      if len(path_vector) < len(pattern_vector):
        return None
      for pattern, path_component in zip(reversed(pattern_vector), reversed(path_vector)):
        if pattern.startswith('%'):
          continue
        if path_component != pattern:
          return None
      matched_path = os.path.join(*path_vector[-len(pattern_vector) + 1:])
      return os.path.normpath(path[:-len(matched_path)])

    while path != os.path.dirname(path):
      root = root_from_path(path)
      if root:
        return root
      path = os.path.dirname(path)

  @classmethod
  def match(cls, path):
    try:
      return cls.EXTRACTOR.parse(path)
    except ScanfParser.ParseError:
      return None

  @classmethod
  def path(cls, result):
    return os.path.join(*cls.PATTERN) % result.groups()

  @classmethod
  def find(cls, root, slave_id='*', framework_id='*', executor_id='*', run='*'):
    mixins = dict(
        root=root, slave_id=slave_id, framework_id=framework_id, executor_id=executor_id, run=run)
    return filter(None, map(cls.match, glob.glob(os.path.join(*cls.PATTERN) % mixins)))

  def __init__(self, root=None):
    self.root = root or self.find_root(os.getcwd())
    if self.root is None:
      raise self.CannotFindRoot('Not a valid executor root!')

  def __iter__(self):
    for extraction in self.find(root=self.root):
      yield extraction
