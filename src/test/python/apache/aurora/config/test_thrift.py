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

import getpass
import re

import pytest

from apache.aurora.config import AuroraConfig
from apache.aurora.config.schema.base import PartitionPolicy as PystachioPartitionPolicy
from apache.aurora.config.schema.base import (
    AppcImage,
    Container,
    Docker,
    DockerImage,
    HealthCheckConfig,
    Job,
    Mesos,
    Metadata,
    Mode,
    Parameter,
    SimpleTask,
    Volume
)
from apache.aurora.config.thrift import convert as convert_pystachio_to_thrift
from apache.aurora.config.thrift import InvalidConfig, task_instance_from_job
from apache.thermos.config.schema import Process, Resources, Task

from gen.apache.aurora.api.constants import GOOD_IDENTIFIER_PATTERN_PYTHON
from gen.apache.aurora.api.ttypes import Mode as ThriftMode
from gen.apache.aurora.api.ttypes import (
    CronCollisionPolicy,
    Identity,
    JobKey,
    PartitionPolicy,
    Resource
)
from gen.apache.aurora.test.constants import INVALID_IDENTIFIERS, VALID_IDENTIFIERS

HELLO_WORLD = Job(
  name='hello_world',
  role='john_doe',
  environment='staging66',
  cluster='smf1-test',
  task=Task(
    name='main',
    processes=[Process(name='hello_world', cmdline='echo {{mesos.instance}}')],
    resources=Resources(cpu=0.1, ram=64 * 1048576, disk=64 * 1048576, gpu=2),
  )
)


def test_simple_config():
  job = convert_pystachio_to_thrift(HELLO_WORLD, ports=frozenset(['health']))
  expected_key = JobKey(
      role=HELLO_WORLD.role().get(),
      environment=HELLO_WORLD.environment().get(),
      name=HELLO_WORLD.name().get())
  assert job.instanceCount == 1
  tti = job.taskConfig
  assert job.key == expected_key
  assert job.owner == Identity(user=getpass.getuser())
  assert job.cronSchedule is None
  assert tti.job == expected_key
  assert tti.isService is False
  assert tti.production is False
  assert tti.priority == 0
  assert tti.maxTaskFailures == 1
  assert tti.constraints == set()
  assert tti.metadata == set()
  assert tti.tier is None
  assert Resource(numCpus=0.1) in list(tti.resources)
  assert Resource(ramMb=64) in list(tti.resources)
  assert Resource(diskMb=64) in list(tti.resources)
  assert Resource(namedPort='health') in list(tti.resources)
  assert Resource(numGpus=2) in list(tti.resources)


def test_config_with_tier():
  config = HELLO_WORLD(tier='devel')
  job = convert_pystachio_to_thrift(config)
  assert job.taskConfig.tier == 'devel'


def test_config_with_docker_image():
  image_name = 'some-image'
  image_tag = 'some-tag'
  job = convert_pystachio_to_thrift(
      HELLO_WORLD(container=Mesos(image=DockerImage(name=image_name, tag=image_tag))))

  assert job.taskConfig.container.mesos.image.appc is None
  assert job.taskConfig.container.mesos.image.docker.name == image_name
  assert job.taskConfig.container.mesos.image.docker.tag == image_tag


def test_config_with_appc_image():
  image_name = 'some-image'
  image_id = 'some-image-id'
  job = convert_pystachio_to_thrift(
          HELLO_WORLD(container=Mesos(image=AppcImage(name=image_name, image_id=image_id))))

  assert job.taskConfig.container.mesos.image.docker is None
  assert job.taskConfig.container.mesos.image.appc.name == image_name
  assert job.taskConfig.container.mesos.image.appc.imageId == image_id


def test_config_with_volumes():
  image_name = 'some-image'
  image_tag = 'some-tag'
  host_path = '/etc/secrets/role/'
  container_path = '/etc/secrets/'

  volume = Volume(host_path=host_path, container_path=container_path, mode=Mode('RO'))

  container = Mesos(image=DockerImage(name=image_name, tag=image_tag), volumes=[volume])

  job = convert_pystachio_to_thrift(HELLO_WORLD(container=container))

  assert len(job.taskConfig.container.mesos.volumes) == 1
  thrift_volume = job.taskConfig.container.mesos.volumes[0]

  assert thrift_volume.hostPath == host_path
  assert thrift_volume.containerPath == container_path
  assert thrift_volume.mode == ThriftMode.RO


def test_docker_with_parameters():
  helloworld = HELLO_WORLD(
    container=Container(
      docker=Docker(image='test_image', parameters=[Parameter(name='foo', value='bar')])
    )
  )
  job = convert_pystachio_to_thrift(helloworld)
  assert job.taskConfig.container.docker.image == 'test_image'


def test_config_with_options():
  hwc = HELLO_WORLD(
    production=True,
    priority=200,
    service=True,
    cron_collision_policy='RUN_OVERLAP',
    partition_policy=PystachioPartitionPolicy(delay_secs=10),
    constraints={
      'dedicated': 'root',
      'cpu': 'x86_64'
    },
    environment='prod'
  )
  job = convert_pystachio_to_thrift(hwc)
  assert job.instanceCount == 1
  tti = job.taskConfig

  assert tti.production
  assert tti.priority == 200
  assert tti.isService
  assert job.cronCollisionPolicy == CronCollisionPolicy.RUN_OVERLAP
  assert len(tti.constraints) == 2
  assert job.key.environment == 'prod'
  assert tti.partitionPolicy == PartitionPolicy(True, 10)


def test_disable_partition_policy():
  hwc = HELLO_WORLD(
    production=True,
    priority=200,
    service=True,
    cron_collision_policy='RUN_OVERLAP',
    partition_policy=PystachioPartitionPolicy(reschedule=False),
    constraints={
      'dedicated': 'root',
      'cpu': 'x86_64'
    },
    environment='prod'
  )
  job = convert_pystachio_to_thrift(hwc)
  assert job.taskConfig.partitionPolicy == PartitionPolicy(False, 0)


def test_config_with_ports():
  hwc = HELLO_WORLD(
    task=HELLO_WORLD.task()(
      processes=[
        Process(name='hello_world',
                cmdline='echo {{thermos.ports[http]}} {{thermos.ports[admin]}}')
      ]
    )
  )
  config = AuroraConfig(hwc)
  job = config.job()
  assert Resource(namedPort='http') in list(job.taskConfig.resources)
  assert Resource(namedPort='admin') in list(job.taskConfig.resources)


def test_config_with_bad_resources():
  MB = 1048576
  hwtask = HELLO_WORLD.task()

  convert_pystachio_to_thrift(HELLO_WORLD)

  good_resources = [
    Resources(cpu=1.0, ram=1 * MB, disk=1 * MB)
  ]

  bad_resources = [
    Resources(cpu=0, ram=1 * MB, disk=1 * MB),
    Resources(cpu=1, ram=0 * MB, disk=1 * MB),
    Resources(cpu=1, ram=1 * MB, disk=0 * MB),
    Resources(cpu=1, ram=1 * MB - 1, disk=1 * MB),
    Resources(cpu=1, ram=1 * MB, disk=1 * MB - 1)
  ]

  for resource in good_resources:
    convert_pystachio_to_thrift(HELLO_WORLD(task=hwtask(resources=resource)))

  for resource in bad_resources:
    with pytest.raises(ValueError):
      convert_pystachio_to_thrift(HELLO_WORLD(task=hwtask(resources=resource)))


def test_unbound_references():
  def job_command(cmdline):
    return AuroraConfig(HELLO_WORLD(task=SimpleTask('hello_world', cmdline))).raw()

  # bindingless and bad => good bindings should work
  convert_pystachio_to_thrift(job_command('echo hello world'))
  convert_pystachio_to_thrift(job_command('echo {{mesos.user}}')
      .bind(mesos={'user': '{{mesos.role}}'}))

  # unbound
  with pytest.raises(InvalidConfig):
    convert_pystachio_to_thrift(job_command('echo {{mesos.user}}'))


def test_cron_collision_policy():
  cron_schedule = '*/10 * * * *'
  CRON_HELLO_WORLD = HELLO_WORLD(cron_schedule=cron_schedule)

  tti = convert_pystachio_to_thrift(CRON_HELLO_WORLD)
  assert tti.cronSchedule == cron_schedule
  assert tti.cronCollisionPolicy == CronCollisionPolicy.KILL_EXISTING

  tti = convert_pystachio_to_thrift(CRON_HELLO_WORLD(cron_collision_policy='RUN_OVERLAP'))
  assert tti.cronSchedule == cron_schedule
  assert tti.cronCollisionPolicy == CronCollisionPolicy.RUN_OVERLAP

  with pytest.raises(ValueError):
    tti = convert_pystachio_to_thrift(CRON_HELLO_WORLD(cron_collision_policy='GARBAGE'))


def test_metadata_in_config():
  job = convert_pystachio_to_thrift(HELLO_WORLD, metadata=[('alpha', 1)])
  assert job.instanceCount == 1
  tti = job.taskConfig

  assert len(tti.metadata) == 1
  pi = iter(tti.metadata).next()
  assert pi.key == 'alpha'
  assert pi.value == '1'


def test_config_with_metadata():
  expected_metadata_tuples = frozenset([("city", "LA"), ("city", "SF")])
  job = convert_pystachio_to_thrift(
      HELLO_WORLD(metadata=[
        Metadata(key=key, value=value)
        for key, value in expected_metadata_tuples]))
  tti = job.taskConfig

  metadata_tuples = frozenset((key_value.key, key_value.value)
                              for key_value in tti.metadata)
  assert metadata_tuples == expected_metadata_tuples


def test_config_with_key_collision_metadata():
  input_metadata_tuples = frozenset([("city", "LA")])
  job = convert_pystachio_to_thrift(
      HELLO_WORLD(metadata=[
        Metadata(key=key, value=value)
        for key, value in input_metadata_tuples]), metadata=[('city', "SF")])
  tti = job.taskConfig

  metadata_tuples = frozenset((key_value.key, key_value.value)
                              for key_value in tti.metadata)
  expected_metadata_tuples = frozenset([("city", "LA"), ("city", "SF")])
  assert metadata_tuples == expected_metadata_tuples


def test_config_with_duplicate_metadata():
  expected_metadata_tuples = frozenset([("city", "LA")])
  job = convert_pystachio_to_thrift(
      HELLO_WORLD(metadata=[
        Metadata(key=key, value=value)
        for key, value in expected_metadata_tuples]), metadata=[('city', "LA")])
  tti = job.taskConfig

  metadata_tuples = frozenset((key_value.key, key_value.value)
                              for key_value in tti.metadata)
  assert metadata_tuples == expected_metadata_tuples


def test_task_instance_from_job():
  instance = task_instance_from_job(
      Job(health_check_config=HealthCheckConfig(interval_secs=30)), 0, '')
  assert instance is not None


def test_identifier_validation():
  matcher = re.compile(GOOD_IDENTIFIER_PATTERN_PYTHON)
  for identifier in VALID_IDENTIFIERS:
    assert matcher.match(identifier)
  for identifier in INVALID_IDENTIFIERS:
    assert not matcher.match(identifier)


def test_mesos_hostname_in_task():
  hw = HELLO_WORLD(task=Task(name="{{mesos.hostname}}"))
  instance = task_instance_from_job(hw, 0, 'test_host')
  assert str(instance.task().name()) == 'test_host'
