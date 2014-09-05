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

from apache.aurora.executor.common.fixtures import BASE_MTI, HELLO_WORLD, HELLO_WORLD_MTI, MESOS_JOB
from apache.aurora.executor.common.task_info import mesos_task_instance_from_assigned_task

from gen.apache.aurora.api.ttypes import AssignedTask, ExecutorConfig, TaskConfig


def test_deserialize_thermos_task():
  task_config = TaskConfig(
      executorConfig=ExecutorConfig(name='thermos', data=MESOS_JOB(task=HELLO_WORLD).json_dumps()))
  assigned_task = AssignedTask(task=task_config, instanceId=0)
  assert mesos_task_instance_from_assigned_task(assigned_task) == BASE_MTI(task=HELLO_WORLD)

  task_config = TaskConfig(
    executorConfig=ExecutorConfig(name='thermos', data=HELLO_WORLD_MTI.json_dumps()))
  assigned_task = AssignedTask(task=task_config, instanceId=0)
  assert mesos_task_instance_from_assigned_task(assigned_task) == BASE_MTI(task=HELLO_WORLD)
