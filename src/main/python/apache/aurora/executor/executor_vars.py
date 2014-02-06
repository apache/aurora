#
# Copyright 2013 Apache Software Foundation
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

import os
import time

import psutil
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import (
    LambdaGauge,
    MutatorGauge,
    NamedGauge,
    Observable)
from twitter.common.python.dirwrapper import PythonDirectoryWrapper
from twitter.common.python.pex import PexInfo
from twitter.common.quantity import Amount, Time
from twitter.common.string.scanf import ScanfParser


class ExecutorVars(Observable, ExceptionalThread):
  """
    Executor exported /vars wrapper.

    Currently writes to disk and communicates through the Aurora Observer,
    pending MESOS-433.
  """
  MUTATOR_METRICS = ('rss', 'cpu', 'thermos_pss', 'thermos_cpu')
  RELEASE_TAG_FORMAT = ScanfParser('%(project)s_R%(release)d')
  DEPLOY_TAG_FORMAT = ScanfParser('%(project)s_%(environment)s_%(release)d_R%(deploy)d')
  PROJECT_NAMES = ('thermos', 'thermos_executor')
  COLLECTION_INTERVAL = Amount(1, Time.MINUTES)

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

  def __init__(self, clock=time):
    self._clock = clock
    self._self = psutil.Process(os.getpid())
    if hasattr(self._self, 'getcwd'):
      self._version = self.get_release_from_binary(
        os.path.join(self._self.getcwd(), self._self.cmdline[1]))
    else:
      self._version = 'UNKNOWN'
    self.metrics.register(NamedGauge('version', self._version))
    self._orphan = False
    self.metrics.register(LambdaGauge('orphan', lambda: int(self._orphan)))
    self._metrics = dict((metric, MutatorGauge(metric, 0)) for metric in self.MUTATOR_METRICS)
    for metric in self._metrics.values():
      self.metrics.register(metric)
    ExceptionalThread.__init__(self)
    self.daemon = True

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
        except psutil.Error:
          continue
    except psutil.Error:
      return

  @classmethod
  def aggregate_memory(cls, process, attribute='pss'):
    try:
      return sum(getattr(mmap, attribute) for mmap in process.get_memory_maps())
    except (psutil.Error, AttributeError):
      # psutil on OS X does not support get_memory_maps
      return 0

  @classmethod
  def cpu_rss_pss(cls, process):
    return (process.get_cpu_percent(0),
            process.get_memory_info().rss,
            cls.aggregate_memory(process, attribute='pss'))

  def run(self):
    while True:
      self._clock.sleep(self.COLLECTION_INTERVAL.as_(Time.SECONDS))
      self.sample()

  def sample(self):
    try:
      executor_cpu, executor_rss, _ = self.cpu_rss_pss(self._self)
      self.write_metric('cpu', executor_cpu)
      self.write_metric('rss', executor_rss)
      self._orphan = self._self.ppid == 1
    except psutil.Error:
      return False

    try:
      child_stats = map(self.cpu_rss_pss, self.thermos_children(self._self))
      self.write_metric('thermos_cpu', sum(stat[0] for stat in child_stats))
      self.write_metric('thermos_pss', sum(stat[2] for stat in child_stats))
    except psutil.Error:
      pass

    return True
