import getpass

from apache.aurora.config.schema.base import (
    MB,
    MesosJob,
    MesosTaskInstance,
    Process,
    Resources,
    Task,
)
from apache.aurora.executor.common.task_info import mesos_task_instance_from_assigned_task

from gen.apache.aurora.ttypes import (
  AssignedTask,
  ExecutorConfig,
  TaskConfig,
)


BASE_MTI = MesosTaskInstance(instance = 0, role = getpass.getuser())
BASE_TASK = Task(resources = Resources(cpu=1.0, ram=16*MB, disk=32*MB))

HELLO_WORLD_TASK_ID = 'hello_world-001'
HELLO_WORLD = BASE_TASK(
    name = 'hello_world',
    processes = [Process(name = 'hello_world_{{thermos.task_id}}', cmdline = 'echo hello world')])
HELLO_WORLD_MTI = BASE_MTI(task=HELLO_WORLD)

SLEEP60 = BASE_TASK(processes = [Process(name = 'sleep60', cmdline = 'sleep 60')])
SLEEP2 = BASE_TASK(processes = [Process(name = 'sleep2', cmdline = 'sleep 2')])
SLEEP60_MTI = BASE_MTI(task=SLEEP60)

MESOS_JOB = MesosJob(
  name = 'does_not_matter',
  instances = 1,
  role = getpass.getuser(),
)

def test_deserialize_thermos_task():
  task_config = TaskConfig(
      executorConfig=ExecutorConfig(name='thermos', data=MESOS_JOB(task=HELLO_WORLD).json_dumps()))
  assigned_task = AssignedTask(task=task_config, instanceId=0)
  assert mesos_task_instance_from_assigned_task(assigned_task) == BASE_MTI(task=HELLO_WORLD)

  task_config = TaskConfig(
    executorConfig=ExecutorConfig(name='thermos', data=HELLO_WORLD_MTI.json_dumps()))
  assigned_task = AssignedTask(task=task_config, instanceId=0)
  assert mesos_task_instance_from_assigned_task(assigned_task) == BASE_MTI(task=HELLO_WORLD)
