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
from io import BytesIO

import pytest
from twitter.common import log
from twitter.common.contextutil import temporary_dir

from apache.aurora.client import config
from apache.aurora.client.config import get_config as get_aurora_config
from apache.aurora.client.config import HTTP_DEPRECATION_WARNING
from apache.aurora.config import AuroraConfig
from apache.aurora.config.loader import AuroraConfigLoader
from apache.aurora.config.schema.base import (
    MB,
    Announcer,
    HealthCheckConfig,
    HealthCheckerConfig,
    Job,
    Resources,
    ShellHealthChecker,
    Task,
    UpdateConfig
)

MESOS_CONFIG_BASE = """
HELLO_WORLD = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'test-cluster',
  environment = 'test',
  %(announce)s
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = '%(cmdline)s')],
    resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB),
  )
)
jobs = [HELLO_WORLD]
"""


MESOS_CONFIG_WITH_INCLUDE = """
%s
include(%s)
"""


MESOS_CONFIG_WITH_ANNOUNCE_1 = MESOS_CONFIG_BASE % {
    'cmdline': 'echo {{thermos.ports[http]}}',
    'announce': 'announce = Announcer(primary_port="http"),'}
MESOS_CONFIG_WITH_ANNOUNCE_2 = MESOS_CONFIG_BASE % {
    'cmdline': 'echo {{thermos.ports[http]}}',
    'announce': '''announce = Announcer(
       primary_port = "http",
       portmap = {"aurora": "http"}),
'''}
MESOS_CONFIG_WITH_INVALID_STATS = MESOS_CONFIG_BASE % {
    'cmdline': 'echo {{thermos.ports[http]}}',
    'announce': 'announce = Announcer(primary_port="http", stats_port="blah"),'}
MESOS_CONFIG_WITHOUT_ANNOUNCE = MESOS_CONFIG_BASE % {
    'cmdline': 'echo {{thermos.ports[http]}}',
    'announce': ''
}


def test_get_config_announces():
  for good_config in (
      MESOS_CONFIG_WITH_ANNOUNCE_1,
      MESOS_CONFIG_WITH_ANNOUNCE_2,
      MESOS_CONFIG_WITHOUT_ANNOUNCE):

    bio = BytesIO(good_config)
    get_aurora_config('hello_world', bio).job()


def test_get_config_with_broken_subscopes():
  bad_config = MESOS_CONFIG_BASE % {
      'cmdline': 'echo {{hello[{{thermos.ports[http]}}]}}',
      'announce': '',
  }
  bio = BytesIO(bad_config)
  with pytest.raises(AuroraConfig.InvalidConfig) as cm:
    get_aurora_config('hello_world', bio).job()
  assert 'Unexpected unbound refs' in str(cm.value.message)


def test_get_config_select():
  bio = BytesIO(MESOS_CONFIG_WITHOUT_ANNOUNCE)

  get_aurora_config(
      'hello_world',
      bio,
      select_env='test',
      select_role='john_doe',
      select_cluster='test-cluster').job()

  bio.seek(0)

  with pytest.raises(ValueError) as cm:
    get_aurora_config(
        'hello_world',
        bio,
        select_env='staging42',
        select_role='moua',
        select_cluster='test-cluster').job()

  assert 'test-cluster/john_doe/test/hello_world' in str(cm.value.message)


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

        get_aurora_config('hello_world', hello_include_fname_path)

        hello_include_fname_fp.seek(0)
        with pytest.raises(AuroraConfigLoader.InvalidConfigError):
          get_aurora_config('hello_world', hello_include_fname_fp)


BAD_ENV = ('Prod', ' prod', 'prod ', 'tEst', 'production', 'staging 2', 'stagingA')
GOOD_ENV = ('prod', 'devel', 'test', 'staging', 'staging001', 'staging1', 'staging1234')


def test_environment_names():
  base_job = Job(
      name='hello_world', role='john_doe', cluster='test-cluster',
      task=Task(name='main', processes=[],
                resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))

  with pytest.raises(ValueError):
    config._validate_environment_name(AuroraConfig(base_job))
  for env_name in GOOD_ENV:
    config._validate_environment_name(AuroraConfig(base_job(environment=env_name)))
  for env_name in BAD_ENV:
    with pytest.raises(ValueError):
      config._validate_environment_name(AuroraConfig(base_job(environment=env_name)))


def test_dedicated_portmap():
  base_job = Job(
      name='hello_world', role='john_doe', cluster='test-cluster',
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


def test_health_check_config_http_ok():
  base_job = Job(
    name='hello_bond', role='james', cluster='marine-cluster',
    health_check_config=HealthCheckConfig(
      max_consecutive_failures=1,
    ),
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))
  config._validate_health_check_config(AuroraConfig(base_job))


def test_health_check_config_shell_ok():
  base_job = Job(
    name='hello_bond', role='james', cluster='marine-cluster',

    health_check_config=HealthCheckConfig(
      max_consecutive_failures=1,
      health_checker=HealthCheckerConfig(
        shell=ShellHealthChecker(shell_command='foo bar')
      )
    ),
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))
  config._validate_health_check_config(AuroraConfig(base_job))


def test_health_check_config_invalid_type():
  # Must be 'shell' or 'http' type of config.
  with pytest.raises(ValueError):
    Job(
      name='hello_bond', role='james', cluster='marine-cluster',
      health_check_config=HealthCheckConfig(
      max_consecutive_failures=1,
      health_checker='foo',
    ),
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))


def test_health_check_config_deprecate_message(monkeypatch):
  base_job = Job(
    name='hello_bond', role='james', cluster='marine-cluster',
    health_check_config=HealthCheckConfig(
      max_consecutive_failures=1,
      endpoint='/to_be_deprecated'
    ),
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))
  log_items = []
  def capture_log(msg):
    log_items.append(msg)
  monkeypatch.setattr(log, 'warn', capture_log)
  config._validate_health_check_config(AuroraConfig(base_job))
  assert log_items == [HTTP_DEPRECATION_WARNING]


def test_health_check_config_shell_no_command():
  # If we chose shell config, we must define shell_command.
  base_job = Job(
    name='hello_bond', role='james', cluster='marine-cluster',
    health_check_config=HealthCheckConfig(
      max_consecutive_failures=1,
      health_checker=HealthCheckerConfig(shell=ShellHealthChecker())
    ),
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))
  with pytest.raises(SystemExit):
    config._validate_health_check_config(AuroraConfig(base_job))


def test_health_check_config_shell_empty_command():
  # shell_command must not be left empty.
  base_job = Job(
      name='hello_bond', role='james', cluster='marine-cluster',
      health_check_config=HealthCheckConfig(
        max_consecutive_failures=1,
        health_checker=HealthCheckerConfig(shell=ShellHealthChecker(shell_command=''))
      ),
      task=Task(name='main', processes=[],
                resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))
  with pytest.raises(SystemExit):
    config._validate_health_check_config(AuroraConfig(base_job))


def test_update_config_passes_with_default_values():
  base_job = Job(
    name='hello_world', role='john_doe', cluster='test-cluster',
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))

  config._validate_update_config(AuroraConfig(base_job))


def test_update_config_passes_with_min_requirement_values():
  base_job = Job(
    name='hello_world', role='john_doe', cluster='test-cluster',
    update_config=UpdateConfig(watch_secs=26),
    health_check_config=HealthCheckConfig(max_consecutive_failures=1),
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))

  config._validate_update_config(AuroraConfig(base_job))


def test_update_config_fails_insufficient_watch_secs_less_than_target():
  base_job = Job(
    name='hello_world', role='john_doe', cluster='test-cluster',
    update_config=UpdateConfig(watch_secs=10),
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))

  with pytest.raises(SystemExit):
    config._validate_update_config(AuroraConfig(base_job))


def test_update_config_fails_insufficient_watch_secs_equal_to_target():
  base_job = Job(
    name='hello_world', role='john_doe', cluster='test-cluster',
    update_config=UpdateConfig(watch_secs=25),
    health_check_config=HealthCheckConfig(max_consecutive_failures=1),
    task=Task(name='main', processes=[],
              resources=Resources(cpu=0.1, ram=64 * MB, disk=64 * MB)))

  with pytest.raises(SystemExit):
    config._validate_update_config(AuroraConfig(base_job))
