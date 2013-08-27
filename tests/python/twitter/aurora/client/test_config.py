import os

from twitter.common.contextutil import temporary_dir, temporary_file

import twitter.aurora.client.jenkins
from twitter.aurora.client import config
from twitter.aurora.client.binding_helpers import apply_binding_helpers
from twitter.aurora.config import AuroraConfig
from twitter.aurora.config.loader import AuroraConfigLoader
from twitter.aurora.config.schema import Announcer, Job, MB, PackerObject, Resources, Task
import twitter.packer.packer_client as packer_client

from gen.twitter.aurora.constants import DEFAULT_ENVIRONMENT

import mox
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

JENKINS_CONFIG = """
jobs = [Job(
  name = 'jenkins-test',
  role = 'some-role',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [ Process(name='command', 
        cmdline='{{jenkins[some-jenkins-project][latest].package}}')],
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


def test_packer_binding_helper():
  mocker = mox.Mox()
  mocker.StubOutWithMock(config.sd_packer_client, 'create_packer')
  mock_client = mocker.CreateMock(packer_client.Packer)
  config.sd_packer_client.create_packer('smf1-test').AndReturn(mock_client)

  pkg = {
    'id': 5,
    'uri': 'hftp://foo/bar/file.zip',
    'auditLog': [{'user': 'john_doe', 'timestamp': 100, 'state': 'PRESENT'}],
  }
  mock_client.get_version('john_doe', 'dummy', 'eleventy').AndReturn(pkg)
  pkg2 = dict(pkg.items())
  pkg2['filename'] = 'different_file.zip'
  mocker.ReplayAll()

  with temporary_file() as fp:
    fp.write(PACKER_CONFIG)
    fp.flush()
    cfg = AuroraConfig.load(fp.name)
    apply_binding_helpers(cfg, {}, False)
    dicts = cfg.binding_dicts.copy()
    # Check that that binding dicts returned by ConfigHelper does
    # indeed contain the appropriate data. We don't so much care about
    # exactly what it says - that's checked later. We just want to
    # make sure that it did save the right data.
    packer_bindings = dicts['packer']
    packer_entry = packer_bindings['packer[john_doe][dummy][eleventy]']
    assert len(packer_entry) == 2
    assert isinstance(packer_entry[0], dict)
    assert isinstance(packer_entry[1], PackerObject)

    # Now, check that the binding dictionary works:
    new_packer_obj = packer_entry[1](package_uri="http://foo/bar.zip", package="bar.zip")
    new_packer_entry = (packer_entry[0], new_packer_obj)
    packer_bindings['packer[john_doe][dummy][eleventy]'] = new_packer_entry
    cfg2 = AuroraConfig.load(fp.name)
    # Update the binding dicts to use for this config.
    cfg2.binding_dicts = dicts
    apply_binding_helpers(cfg2, {}, False)
    dicts2 = cfg2.binding_dicts
    assert dicts == dicts2
    orig_job = cfg._job
    binding_job = cfg2._job
    # check that unmodified fields from the binding dict still match
    # the original
    assert orig_job.instances() == binding_job.instances()
    assert orig_job.priority() == binding_job.priority()
    # check that the original config wasn't affected by changing the binding dict.
    assert str(orig_job.task().processes()[0].cmdline()) == "file.zip"
    # check that the config populated by the binding dict got the correct value
    # from the dict.
    assert str(binding_job.task().processes()[0].cmdline()) == "bar.zip"
    mocker.VerifyAll()


def test_jenkins_binding_helper():
  mocker = mox.Mox()

  # Replace the standard packer with a test mock.
  mocker.StubOutWithMock(config.sd_packer_client, 'create_packer')
  mock_packer = mocker.CreateMock(packer_client.Packer)
  config.sd_packer_client.create_packer('smf1-test').AndReturn(mock_packer)

  # The jenkins helper calls the JenkinsArtifactResolver.
  # We need to capture that call, so we stub out the artifact resolver's resolve method.
  mocker.StubOutWithMock(twitter.aurora.client.jenkins.JenkinsArtifactResolver, 'resolve')
  mocker.StubOutWithMock(twitter.aurora.client.jenkins.JenkinsArtifactResolver, '__init__')
  twitter.aurora.client.jenkins.JenkinsArtifactResolver.__init__(mock_packer, 'some-role')
  expected_pkg_data = {'auditLog': [{'timestamp': 100, 'state': 'PRESENT', 'user': 'john_doe'}],
                     'id': 5, 'uri': 'hftp://foo/bar/file.zip'}
  twitter.aurora.client.jenkins.JenkinsArtifactResolver.resolve(
      'some-jenkins-project', 'latest').AndReturn(('package', expected_pkg_data))
  mocker.ReplayAll()

  with temporary_file() as fp:
    fp.write(JENKINS_CONFIG)
    fp.flush()
    cfg = AuroraConfig.load(fp.name)
    apply_binding_helpers(cfg, {}, False)
    # Check that that binding dicts returned by ConfigHelper does
    # indeed contain the appropriate data. We don't so much care about
    # exactly what it says - that's checked later. We just want to
    # make sure that it did save the right data.
    assert 'file.zip' == cfg.task(0).processes()[0].cmdline().get()

    # Check the contents of the binding dict.
    jenkins_bindings = cfg.binding_dicts['jenkins']
    jenkins_entry = jenkins_bindings['jenkins[some-jenkins-project][latest]']
    assert len(jenkins_entry) == 2
    assert jenkins_entry['data']['uri'] == 'hftp://foo/bar/file.zip'
  mocker.VerifyAll()
