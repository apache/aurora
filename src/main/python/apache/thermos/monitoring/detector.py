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

"""Detect Thermos tasks on disk

This module contains the TaskDetector, used to detect Thermos tasks within a given checkpoint root.

"""

import functools
import glob
import os
import re
from abc import abstractmethod

from twitter.common.lang import Compatibility, Interface

from apache.thermos.common.constants import DEFAULT_CHECKPOINT_ROOT
from apache.thermos.common.path import TaskPath


class PathDetector(Interface):
  @abstractmethod
  def get_paths(self):
    """Get a list of valid checkpoint roots."""


class FixedPathDetector(PathDetector):
  def __init__(self, path=DEFAULT_CHECKPOINT_ROOT):
    if not isinstance(path, Compatibility.string):
      raise TypeError('FixedPathDetector path should be a string, got %s' % type(path))
    self._paths = [path]

  def get_paths(self):
    return self._paths[:]


class ChainedPathDetector(PathDetector):
  def __init__(self, *detectors):
    for detector in detectors:
      if not isinstance(detector, PathDetector):
        raise TypeError('Expected detector %r to be a PathDetector, got %s' % (
            detector, type(detector)))
    self._detectors = detectors

  def get_paths(self):
    def iterate():
      for detector in self._detectors:
        for path in detector.get_paths():
          yield path
    return list(set(iterate()))


def memoized(fn):
  cache_attr_name = '__memoized_' + fn.__name__

  @functools.wraps(fn)
  def memoized_fn(self, *args):
    if not hasattr(self, cache_attr_name):
      setattr(self, cache_attr_name, {})
    cache = getattr(self, cache_attr_name)
    try:
      return cache[args]
    except KeyError:
      cache[args] = rv = fn(self, *args)
      return rv

  return memoized_fn


class TaskDetector(object):
  """
    Helper class in front of TaskPath to detect active/finished/running tasks. Performs no
    introspection on the state of a task; merely detects based on file paths on disk.
  """
  class Error(Exception): pass
  class MatchingError(Error): pass

  def __init__(self, root):
    self._root_dir = root
    self._pathspec = TaskPath()

  @memoized
  def __get_task_ids_patterns(self, state):
    path_glob = self._pathspec.given(
        root=self._root_dir,
        task_id="*",
        state=state or '*'
    ).getpath('task_path')
    path_regex = self._pathspec.given(
        root=re.escape(self._root_dir),
        task_id=r'(\S+)',
        state=r'(\S+)'
    ).getpath('task_path')
    return path_glob, re.compile(path_regex)

  def get_task_ids(self, state=None):
    path_glob, path_regex = self.__get_task_ids_patterns(state)
    for path in glob.glob(path_glob):
      try:
        task_state, task_id = path_regex.match(path).groups()
      except Exception:
        continue
      if state is None or task_state == state:
        yield (task_state, task_id)

  @memoized
  def __get_process_runs_patterns(self, task_id, log_dir):
    path_glob = self._pathspec.given(
        root=self._root_dir,
        task_id=task_id,
        log_dir=log_dir,
        process='*',
        run='*'
    ).getpath('process_logdir')
    path_regex = self._pathspec.given(
        root=re.escape(self._root_dir),
        task_id=re.escape(task_id),
        log_dir=log_dir,
        process=r'(\S+)',
        run=r'(\d+)'
    ).getpath('process_logdir')
    return path_glob, re.compile(path_regex)

  def get_process_runs(self, task_id, log_dir):
    path_glob, path_regex = self.__get_process_runs_patterns(task_id, log_dir)
    for path in glob.glob(path_glob):
      try:
        process, run = path_regex.match(path).groups()
      except Exception:
        continue
      yield process, int(run)

  def get_process_logs(self, task_id, log_dir):
    for process, run in self.get_process_runs(task_id, log_dir):
      for logtype in ('stdout', 'stderr'):
        path = (self._pathspec.with_filename(logtype).given(root=self._root_dir,
                                                            task_id=task_id,
                                                            log_dir=log_dir,
                                                            process=process,
                                                            run=run)
                                                     .getpath('process_logdir'))
        if os.path.exists(path):
          yield path

  def get_checkpoint(self, task_id):
    return self._pathspec.given(root=self._root_dir, task_id=task_id).getpath('runner_checkpoint')

  @memoized
  def __get_process_checkpoints_patterns(self, task_id):
    path_glob = self._pathspec.given(
        root=self._root_dir,
        task_id=task_id,
        process='*'
    ).getpath('process_checkpoint')
    path_regex = self._pathspec.given(
        root=re.escape(self._root_dir),
        task_id=re.escape(task_id),
        process=r'(\S+)',
    ).getpath('process_checkpoint')
    return path_glob, re.compile(path_regex)

  def get_process_checkpoints(self, task_id):
    path_glob, path_regex = self.__get_process_checkpoints_patterns(task_id)
    for path in glob.glob(path_glob):
      try:
        process, = path_regex.match(path).groups()
      except Exception:
        continue
      yield path
