import os

from twitter.common.contextutil import temporary_dir, temporary_file

from apache.aurora.client import config
from apache.aurora.config import AuroraConfig
from apache.aurora.config.loader import AuroraConfigLoader
from apache.aurora.config.schema.base import Announcer, Job, MB, Resources, Task

from gen.apache.aurora.constants import DEFAULT_ENVIRONMENT

import pytest


MESOS_CONFIG_BASE = """
HELLO_WORLD = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  environment = 'test',
  %s
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = 'echo {{thermos.ports[http]}}')],
    resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB),
  )
)
jobs = [HELLO_WORLD]
"""


MESOS_CONFIG_WITH_INCLUDE = """
%s
include(%s)
"""


MESOS_CONFIG_WITH_ANNOUNCE_1 = MESOS_CONFIG_BASE % 'announce = Announcer(primary_port="http"),'
MESOS_CONFIG_WITH_ANNOUNCE_2 = MESOS_CONFIG_BASE % (
 '''announce = Announcer(
       primary_port = "http",
       portmap = {"aurora": "http"}),
 ''')
MESOS_CONFIG_WITH_INVALID_STATS = MESOS_CONFIG_BASE % (
    'announce = Announcer(primary_port="http", stats_port="blah"),')
MESOS_CONFIG_WITHOUT_ANNOUNCE = MESOS_CONFIG_BASE % ''


def test_get_config_announces():
  for good_config in (MESOS_CONFIG_WITH_ANNOUNCE_1, MESOS_CONFIG_WITH_ANNOUNCE_2,
                      MESOS_CONFIG_WITHOUT_ANNOUNCE):
    with temporary_file() as fp:
      fp.write(good_config)
      fp.flush()
      config.get_config('hello_world', fp.name)

      fp.seek(0)
      config.get_config('hello_world', fp)


def test_get_config_select():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG_BASE % '')
    fp.flush()

    fp.seek(0)
    config.get_config(
        'hello_world', fp, select_env='test',
        select_role='john_doe', select_cluster='smf1-test')

    fp.seek(0)
    with pytest.raises(ValueError) as cm:
      config.get_config(
          'hello_world', fp, select_env='staging42',
          select_role='moua', select_cluster='smf1-test')
    assert 'smf1-test/john_doe/test/hello_world' in str(cm.value.message)


def test_include():
  with temporary_dir() as dir:
    hello_mesos_fname = "hello_world.mesos"
    hello_mesos_path = os.path.join(dir, hello_mesos_fname)
    with open(os.path.join(dir, hello_mesos_path), "wb") as hello_world_mesos:
      hello_world_mesos.write(MESOS_CONFIG_WITHOUT_ANNOUNCE)
      hello_world_mesos.flush()

      hello_include_fname_path = os.path.join(dir, "hello_include_fname.mesos")
      with open(hello_include_fname_path, "wb+") as hello_include_fname_fp:
        hello_include_fname_fp.write(MESOS_CONFIG_WITH_INCLUDE %
            ("", """'%s'""" % hello_mesos_fname))
        hello_include_fname_fp.flush()

        config.get_config('hello_world', hello_include_fname_path)

        hello_include_fname_fp.seek(0)
        with pytest.raises(AuroraConfigLoader.InvalidConfigError):
          config.get_config('hello_world', hello_include_fname_fp)


def test_environment_names():
  BAD = ('Prod', ' prod', 'prod ', 'tEst', 'production', 'staging 2', 'stagingA')
  GOOD = ('prod', 'devel', 'test', 'staging', 'staging001', 'staging1', 'staging1234')
  base_job = Job(
      name='hello_world', role='john_doe', cluster='smf1-test',
      task=Task(name='main', processes=[],
                resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))

  with pytest.raises(ValueError):
    config._validate_environment_name(AuroraConfig(base_job))
  for env_name in GOOD:
    config._validate_environment_name(AuroraConfig(base_job(environment=env_name)))
  for env_name in BAD:
    with pytest.raises(ValueError):
      config._validate_environment_name(AuroraConfig(base_job(environment=env_name)))


def test_inject_default_environment():
  base_job = Job(
      name='hello_world', role='john_doe', cluster='smf1-test',
      task=Task(name='main', processes=[],
                resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))

  no_env_config = AuroraConfig(base_job)
  config._inject_default_environment(no_env_config)
  assert no_env_config.environment() == DEFAULT_ENVIRONMENT

  test_env_config = AuroraConfig(base_job(environment='test'))
  config._inject_default_environment(test_env_config)
  assert test_env_config.environment() == 'test'


def test_dedicated_portmap():
  base_job = Job(
      name='hello_world', role='john_doe', cluster='smf1-test',
      task=Task(name='main', processes=[],
                resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))

  config._validate_announce_configuration(AuroraConfig(base_job))
  config._validate_announce_configuration(
      AuroraConfig(base_job(constraints={'dedicated': 'mesos-team'})))
  config._validate_announce_configuration(
      AuroraConfig(base_job(constraints={'dedicated': 'mesos-team'},
                            announce=Announcer(portmap={'http': 80}))))

  with pytest.raises(ValueError):
    config._validate_announce_configuration(
        AuroraConfig(base_job(announce=Announcer(portmap={'http': 80}))))

  with pytest.raises(ValueError):
    config._validate_announce_configuration(
        AuroraConfig(base_job(announce=Announcer(portmap={'http': 80}),
                              constraints={'foo': 'bar'})))
