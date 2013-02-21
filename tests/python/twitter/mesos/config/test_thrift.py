import copy
import getpass
from contextlib import contextmanager

import pytest

from gen.twitter.mesos.ttypes import (
  Constraint,
  CronCollisionPolicy,
  Identity,
  TaskConstraint,
  ValueConstraint,
)
from twitter.common.contextutil import temporary_file
from twitter.mesos.config.schema import (
  MesosJob,
)
from twitter.mesos.config.thrift import convert as convert_pystachio_to_thrift
from twitter.thermos.config.schema import (
  Process,
  Resources,
  Task
)

from pystachio import Empty, Integer, Map, String


HELLO_WORLD = MesosJob(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = 'echo {{mesos.instance}}')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)


def test_simple_config():
  job = convert_pystachio_to_thrift(HELLO_WORLD)
  assert len(job.taskConfigs) == 1
  tti = iter(job.taskConfigs).next()


  assert job.name == 'hello_world'
  assert job.owner == Identity(role=HELLO_WORLD.role().get(), user=getpass.getuser())
  assert job.cronSchedule == ''

  assert tti.jobName == 'hello_world'
  assert tti.isDaemon == False
  assert tti.numCpus == 0.1
  assert tti.ramMb == 64
  assert tti.diskMb == 64
  assert tti.requestedPorts == set()
  assert tti.production == False
  assert tti.priority == 0
  assert tti.healthCheckIntervalSecs == 30
  assert tti.maxTaskFailures == 1
  assert tti.shardId == 0
  assert tti.constraints == set()


def test_config_with_options():
  hwc = HELLO_WORLD(
    production = True,
    priority = 200,
    daemon = True,
    health_check_interval_secs = 30,
    cron_policy = 'RUN_OVERLAP',
    constraints = {
      'dedicated': 'your_mom',
      'cpu': 'x86_64'
    }
  )
  job = convert_pystachio_to_thrift(hwc)
  assert len(job.taskConfigs) == 1
  tti = iter(job.taskConfigs).next()

  assert tti.production == True
  assert tti.priority == 200
  assert tti.isDaemon == True
  assert job.cronCollisionPolicy == CronCollisionPolicy.RUN_OVERLAP
  assert tti.healthCheckIntervalSecs == 30
  assert len(tti.constraints) == 2


def test_config_with_ports():
  hwc = HELLO_WORLD(
    task = HELLO_WORLD.task()(
      processes = [
        Process(name = 'hello_world',
                cmdline = 'echo {{thermos.ports[http]}} {{thermos.ports[admin]}}')
      ]
    )
  )
  job = convert_pystachio_to_thrift(hwc)
  assert len(job.taskConfigs) == 1
  tti = iter(job.taskConfigs).next()
  assert tti.requestedPorts == set(['http', 'admin'])


def test_config_with_bad_resources():
  MB = 1048576
  hwtask = HELLO_WORLD.task()

  convert_pystachio_to_thrift(HELLO_WORLD)

  good_resources = [
    Resources(cpu = 1.0, ram = 1 * MB, disk = 1 * MB)
  ]

  bad_resources = [
    Resources(cpu = 0, ram = 1 * MB, disk = 1 * MB),
    Resources(cpu = 1, ram = 0 * MB, disk = 1 * MB),
    Resources(cpu = 1, ram = 1 * MB, disk = 0 * MB),
    Resources(cpu = 1, ram = 1 * MB - 1, disk = 1 * MB),
    Resources(cpu = 1, ram = 1 * MB, disk = 1 * MB - 1)
  ]

  for resource in good_resources:
    convert_pystachio_to_thrift(HELLO_WORLD(task = hwtask(resources = resource)))

  for resource in bad_resources:
    with pytest.raises(ValueError):
      convert_pystachio_to_thrift(HELLO_WORLD(task = hwtask(resources = resource)))


def test_cron_policy_alias():
  cron_schedule = '*/10 * * * *'
  CRON_HELLO_WORLD = HELLO_WORLD(cron_schedule=cron_schedule)

  tti = convert_pystachio_to_thrift(CRON_HELLO_WORLD)
  assert tti.cronSchedule == cron_schedule
  assert tti.cronCollisionPolicy == CronCollisionPolicy.KILL_EXISTING

  tti = convert_pystachio_to_thrift(CRON_HELLO_WORLD(cron_policy='RUN_OVERLAP'))
  assert tti.cronSchedule == cron_schedule
  assert tti.cronCollisionPolicy == CronCollisionPolicy.RUN_OVERLAP

  tti = convert_pystachio_to_thrift(CRON_HELLO_WORLD(cron_collision_policy='RUN_OVERLAP'))
  assert tti.cronSchedule == cron_schedule
  assert tti.cronCollisionPolicy == CronCollisionPolicy.RUN_OVERLAP

  with pytest.raises(ValueError):
    tti = convert_pystachio_to_thrift(CRON_HELLO_WORLD(cron_policy='RUN_OVERLAP',
                                                       cron_collision_policy='RUN_OVERLAP'))

  with pytest.raises(ValueError):
    tti = convert_pystachio_to_thrift(CRON_HELLO_WORLD(cron_collision_policy='GARBAGE'))

def test_packages_in_config():
  job = convert_pystachio_to_thrift(HELLO_WORLD, packages = [('alpha', 'beta', 1)])
  assert len(job.taskConfigs) == 1
  tti = iter(job.taskConfigs).next()

  assert len(tti.packages) == 1
  pi = iter(tti.packages).next()
  assert pi.role == 'alpha'
  assert pi.name == 'beta'
  assert pi.version == 1
