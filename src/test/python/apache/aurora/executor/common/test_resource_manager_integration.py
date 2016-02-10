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
import threading

import mock
from mesos.interface import mesos_pb2
from twitter.common.contextutil import temporary_dir

from apache.aurora.executor.common.resource_manager import ResourceManagerProvider
from apache.aurora.executor.common.sandbox import DirectorySandbox
from apache.thermos.common.path import TaskPath
from apache.thermos.core.helper import TaskRunnerHelper
from apache.thermos.monitoring.disk import DiskCollector

from gen.apache.thermos.ttypes import RunnerCkpt, RunnerHeader


# TODO(jcohen): There should really be a single canonical source for creating test jobs/tasks
def make_assigned_task(thermos_config, assigned_ports=None):
  from gen.apache.aurora.api.constants import AURORA_EXECUTOR_NAME
  from gen.apache.aurora.api.ttypes import AssignedTask, ExecutorConfig, JobKey, TaskConfig

  assigned_ports = assigned_ports or {}
  executor_config = ExecutorConfig(name=AURORA_EXECUTOR_NAME, data=thermos_config.json_dumps())
  task_config = TaskConfig(
      job=JobKey(
          role=thermos_config.role().get(),
          environment='test',
          name=thermos_config.name().get()),
      executorConfig=executor_config)

  return AssignedTask(
      instanceId=12345,
      task=task_config,
      assignedPorts=assigned_ports,
      taskId="taskId-12345")


def make_job(role, environment, name, primary_port, portmap):
  from apache.aurora.config.schema.base import (
      Announcer,
      Job,
      Process,
      Resources,
      Task,
  )
  task = Task(
      name='ignore2',
      processes=[Process(name='ignore3', cmdline='ignore4')],
      resources=Resources(cpu=1, ram=1, disk=1))
  job = Job(
      role=role,
      environment=environment,
      name=name,
      cluster='ignore1',
      task=task,
      announce=Announcer(primary_port=primary_port, portmap=portmap))
  return job


def write_header(root, sandbox, task_id):
  log_dir = os.path.join(sandbox, '.logs')
  path = TaskPath(root=root, task_id=task_id, log_dir=log_dir)
  header = RunnerHeader(task_id=task_id, sandbox=sandbox, log_dir=log_dir)
  ckpt = TaskRunnerHelper.open_checkpoint(path.getpath('runner_checkpoint'))
  ckpt.write(RunnerCkpt(runner_header=header))
  ckpt.close()


def test_resource_manager():
  with temporary_dir() as td:
    assigned_task = make_assigned_task(
        make_job('some-role', 'some-env', 'some-job', 'http', portmap={'http': 80}))
    sandbox = os.path.join(td, 'sandbox')
    root = os.path.join(td, 'thermos')
    write_header(root, sandbox, assigned_task.taskId)

    mock_disk_collector_class = mock.create_autospec(DiskCollector, spec_set=True)
    mock_disk_collector = mock_disk_collector_class.return_value

    mock_disk_collector.sample.return_value = None
    value_mock = mock.PropertyMock(return_value=4197)
    type(mock_disk_collector).value = value_mock

    completed_event = threading.Event()
    completed_mock = mock.PropertyMock(return_value=completed_event)
    type(mock_disk_collector).completed_event = completed_mock

    rmp = ResourceManagerProvider(root, disk_collector=mock_disk_collector_class)
    rm = rmp.from_assigned_task(assigned_task, DirectorySandbox(sandbox))

    assert rm.status is None

    try:
      rm.start()
      while rm.status is None:
        rm._kill_event.wait(timeout=1)
      result = rm.status
      assert result is not None
      assert result.reason.startswith('Disk limit exceeded')
      assert result.status == mesos_pb2.TASK_FAILED
      assert value_mock.call_count == 1
      assert completed_mock.call_count == 1
      assert mock_disk_collector.sample.call_count == 1
    finally:
      rm._resource_monitor.kill()
