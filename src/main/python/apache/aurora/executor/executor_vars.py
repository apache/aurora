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
from twitter.common.metrics import LambdaGauge, MutatorGauge, Observable
from twitter.common.quantity import Amount, Time


class ExecutorVars(Observable, ExceptionalThread):
  """
    Executor exported /vars wrapper.

    Currently writes to disk and communicates through the Aurora Observer,
    pending MESOS-433.
  """
  MUTATOR_METRICS = ('rss', 'cpu', 'thermos_pss', 'thermos_cpu')
  COLLECTION_INTERVAL = Amount(1, Time.MINUTES)

  def __init__(self, clock=time):
    self._clock = clock
    self._self = psutil.Process(os.getpid())
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
      for child in parent.children():
        yield child  # thermos_runner
        try:
          for grandchild in child.children():
            yield grandchild  # thermos_coordinator
        except psutil.Error:
          continue
    except psutil.Error:
      return

  @classmethod
  def aggregate_memory(cls, process, attribute='pss'):
    try:
      return sum(getattr(mmap, attribute) for mmap in process.memory_maps())
    except (psutil.Error, AttributeError):
      # psutil on OS X does not support get_memory_maps
      return 0

  @classmethod
  def cpu_rss_pss(cls, process):
    return (process.cpu_percent(0),
            process.memory_info().rss,
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
      self._orphan = self._self.ppid() == 1
    except psutil.Error:
      return False

    try:
      child_stats = map(self.cpu_rss_pss, self.thermos_children(self._self))
      self.write_metric('thermos_cpu', sum(stat[0] for stat in child_stats))
      self.write_metric('thermos_pss', sum(stat[2] for stat in child_stats))
    except psutil.Error:
      pass

    return True
