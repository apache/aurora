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

from mesos.interface import mesos_pb2
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_open

from apache.aurora.executor.common.resource_manager import ResourceManagerProvider
from apache.aurora.executor.common.sandbox import DirectorySandbox


# TODO(jcohen): There should really be a single canonical source for creating test jobs/tasks
def make_assigned_task(thermos_config, assigned_ports=None):
  from gen.apache.aurora.api.constants import AURORA_EXECUTOR_NAME
  from gen.apache.aurora.api.ttypes import AssignedTask, ExecutorConfig, Identity, TaskConfig

  assigned_ports = assigned_ports or {}
  executor_config = ExecutorConfig(name=AURORA_EXECUTOR_NAME, data=thermos_config.json_dumps())
  task_config = TaskConfig(
    owner=Identity(role=thermos_config.role().get(), user=thermos_config.role().get()),
    environment=thermos_config.environment().get(),
    jobName=thermos_config.name().get(),
    executorConfig=executor_config)

  return AssignedTask(instanceId=12345, task=task_config, assignedPorts=assigned_ports)


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


def test_resource_manager():
  with temporary_dir() as td:
    sandbox = os.path.join(td, 'sandbox')
    root = os.path.join(td, 'thermos')

    rm = ResourceManagerProvider(root).from_assigned_task(
        make_assigned_task(
            make_job('some-role', 'some-env', 'some-job', 'http', portmap={'http': 80})),
        DirectorySandbox(sandbox))

    assert rm.status is None

    try:
      rm.start()

      with safe_open(os.path.join(sandbox, 'foo.txt'), 'w') as fp:
        fp.write(4097 * 'x')

      # N.B. The sleep below makes this test inherently vulnerable to flakiness
      # TODO(jcohen): Investigate using threading.event to avoid the need for a timeout entirely.
      timeout = 0
      while rm.status is None and timeout < 5:
        timeout += 0.1
        time.sleep(0.1)

      result = rm.status
      assert result is not None
      assert result.reason.startswith('Disk limit exceeded')
      assert result.status == mesos_pb2.TASK_FAILED
    finally:
      rm._resource_monitor.kill()
