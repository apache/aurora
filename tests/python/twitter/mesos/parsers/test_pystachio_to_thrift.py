import copy
import getpass
from contextlib import contextmanager

from gen.twitter.mesos.ttypes import (
  Constraint,
  CronCollisionPolicy,
  Identity,
  TaskConstraint,
  ValueConstraint,
)
from twitter.common.contextutil import temporary_file
from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import (
  MesosJob,
)
from twitter.mesos.parsers.pystachio_thrift import convert as convert_pystachio_to_thrift
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
    processes = [Process(name = 'hello_world', cmdline = 'echo hello world')],
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
  # This is apparently not possible:
  #
  # assert tti.constraints == set([
  #  Constraint(name = 'cpu',
  #             constraint = TaskConstraint(
  #                 value = ValueConstraint(negated = False, values = set(['x86_64'])))),
  #  Constraint(name = 'dedicated',
  #             constraint = TaskConstraint(
  #                 value = ValueConstraint(negated = False, values = set(['your_mom']))))
  #])
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
