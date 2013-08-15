import json
import os
import tempfile

from twitter.common.contextutil import temporary_dir, temporary_file, open_zip
from twitter.mesos.client import config
from twitter.mesos.config import AuroraConfig
from twitter.mesos.config.loader import AuroraConfigLoader
from twitter.mesos.config.schema import Announcer, Job, Resources, Task, MB
from twitter.mesos.packer import sd_packer_client
import twitter.mesos.packer.packer_client as packer_client

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT

import mox
from mox import Mox, IsA
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

PACKER_CONFIG = """
jobs = [Job(
  name = 'hello_world',
  role = 'john_doe',
  environment = 'staging42',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name='command', cmdline="{{packer[john_doe][dummy][eleventy].package}}")],
    resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB),
  )
)]
"""

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
        hello_include_fname_fp.write(MESOS_CONFIG_WITH_INCLUDE % ("", """'%s'""" % hello_mesos_fname))
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
      task = Task(name='main', processes = [],
                  resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB)))

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
      task = Task(name='main', processes = [],
                  resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB)))

  no_env_config = AuroraConfig(base_job)
  config._inject_default_environment(no_env_config)
  assert no_env_config.environment() == DEFAULT_ENVIRONMENT

  test_env_config = AuroraConfig(base_job(environment='test'))
  config._inject_default_environment(test_env_config)
  assert test_env_config.environment() == 'test'


def test_dedicated_portmap():
  base_job = Job(
      name='hello_world', role='john_doe', cluster='smf1-test',
      task = Task(name='main', processes = [],
                  resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB)))

  config._validate_announce_configuration(AuroraConfig(base_job))
  config._validate_announce_configuration(
      AuroraConfig(base_job(constraints = {'dedicated': 'mesos-team'})))
  config._validate_announce_configuration(
      AuroraConfig(base_job(constraints = {'dedicated': 'mesos-team'},
                               announce = Announcer(portmap={'http': 80}))))

  with pytest.raises(ValueError):
    config._validate_announce_configuration(
        AuroraConfig(base_job(announce=Announcer(portmap={'http': 80}))))

  with pytest.raises(ValueError):
    config._validate_announce_configuration(
        AuroraConfig(base_job(announce=Announcer(portmap={'http': 80}),
                                 constraints = {'foo': 'bar'})))

def test_package_filename():
  mocker = mox.Mox()
  mocker.StubOutWithMock(config.sd_packer_client, 'create_packer')
  mock_client = mocker.CreateMock(packer_client.Packer)
  config.sd_packer_client.create_packer('smf1-test').AndReturn(mock_client)
  config.sd_packer_client.create_packer('smf1-test').AndReturn(mock_client)

  pkg = {
    'id': 5,
    'uri': 'hftp://foo/bar/file.zip',
    'auditLog': [{'user': 'john_doe', 'timestamp': 100, 'state': 'PRESENT'}],
  }
  mock_client.get_version('john_doe', 'dummy', 'eleventy').AndReturn(pkg)
  pkg2 = dict(pkg.items())
  pkg2['filename'] = 'different_file.zip'
  mock_client.get_version('john_doe', 'dummy', 'eleventy').AndReturn(pkg2)
  mocker.ReplayAll()

  with temporary_file() as fp:
    fp.write(PACKER_CONFIG)
    fp.flush()
    cfg = config.get_config('hello_world', fp.name)
    assert 'file.zip' == cfg.task(0).processes()[0].cmdline().get()

  with temporary_file() as fp:
    fp.write(PACKER_CONFIG)
    fp.flush()
    cfg = config.get_config('hello_world', fp.name)
    assert 'different_file.zip' == cfg.task(0).processes()[0].cmdline().get()
