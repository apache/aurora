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

import threading

from mesos.interface import mesos_pb2
from twitter.common.metrics import LambdaGauge
from twitter.common.quantity import Amount, Time

from apache.aurora.executor.common.status_checker import (
    StatusChecker,
    StatusCheckerProvider,
    StatusResult
)
from apache.aurora.executor.common.task_info import mesos_task_instance_from_assigned_task
from apache.thermos.common.path import TaskPath
from apache.thermos.monitoring.disk import DiskCollector
from apache.thermos.monitoring.monitor import TaskMonitor
from apache.thermos.monitoring.resource import TaskResourceMonitor


class ResourceManager(StatusChecker):
  """ Manage resources consumed by a Task """

  def __init__(self, resources, resource_monitor):
    """
      resources: Resources object specifying cpu, ram, disk limits for the task
      resource_monitor: The ResourceMonitor to monitor resources
    """
    self._resource_monitor = resource_monitor
    # TODO(wickman) Remove cpu/ram reporting if MESOS-1458 is resolved.
    self._max_cpu = resources.cpu().get()
    self._max_ram = resources.ram().get()
    self._max_disk = resources.disk().get()
    self._kill_reason = None
    self._kill_event = threading.Event()

  @property
  def _num_procs(self):
    """ Total number of processes the task consists of (including child processes) """
    return self._resource_monitor.sample()[1].num_procs

  @property
  def _ps_sample(self):
    """ ProcessSample representing the aggregate resource consumption of the Task's processes """
    return self._resource_monitor.sample()[1].process_sample

  @property
  def _disk_sample(self):
    """ Integer in bytes representing the disk consumption in the Task's sandbox """
    return self._resource_monitor.sample()[1].disk_usage

  @property
  def status(self):
    sample = self._disk_sample
    if sample > self._max_disk:
      self._kill_event.set()
      return StatusResult('Disk limit exceeded.  Reserved %s bytes vs used %s bytes.' % (
          self._max_disk, sample), mesos_pb2.TASK_FAILED)

  def name(self):
    return 'resource_manager'

  def register_metrics(self):
    self.metrics.register(LambdaGauge('disk_used', lambda: self._disk_sample))
    self.metrics.register(LambdaGauge('disk_reserved', lambda: self._max_disk))
    self.metrics.register(LambdaGauge('disk_percent',
        lambda: 1.0 * self._disk_sample / self._max_disk))
    self.metrics.register(LambdaGauge('cpu_used', lambda: self._ps_sample.rate))
    self.metrics.register(LambdaGauge('cpu_reserved', lambda: self._max_cpu))
    self.metrics.register(LambdaGauge('cpu_percent',
        lambda: 1.0 * self._ps_sample.rate / self._max_cpu))
    self.metrics.register(LambdaGauge('ram_used', lambda: self._ps_sample.rss))
    self.metrics.register(LambdaGauge('ram_reserved', lambda: self._max_ram))
    self.metrics.register(LambdaGauge('ram_percent',
        lambda: 1.0 * self._ps_sample.rss / self._max_ram))

  def start(self):
    super(ResourceManager, self).start()
    self.register_metrics()
    self._resource_monitor.start()


class ResourceManagerProvider(StatusCheckerProvider):
  def __init__(self,
               checkpoint_root,
               disk_collector=DiskCollector,
               disk_collection_interval=Amount(1, Time.MINUTES)):
    self._checkpoint_root = checkpoint_root
    self._disk_collector = disk_collector
    self._disk_collection_interval = disk_collection_interval

  def from_assigned_task(self, assigned_task, sandbox):
    task_id = assigned_task.taskId
    resources = mesos_task_instance_from_assigned_task(assigned_task).task().resources()
    task_path = TaskPath(root=self._checkpoint_root, task_id=task_id)
    task_monitor = TaskMonitor(task_path, task_id)
    resource_monitor = TaskResourceMonitor(
        task_monitor,
        sandbox.root,
        disk_collector=self._disk_collector,
        disk_collection_interval=self._disk_collection_interval)
    return ResourceManager(resources, resource_monitor)
