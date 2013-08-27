import getpass
import hashlib

from twitter.mesos.client.quickrun import Quickrun
from twitter.mesos.config.schema import Packer
from twitter.packer import sd_packer_client


import mox
import pytest


class UnpopulatedQuickrun(Quickrun):
  def populate_namespaces(self, config):
    return config


class MockedMD5Quickrun(UnpopulatedQuickrun):
  MD5_VALUE = None

  @classmethod
  def get_md5(cls, filename):
    return cls.MD5_VALUE


def test_no_packages():
  qr = UnpopulatedQuickrun(
      'smf1',             # cluster
      'echo hello world', # command
      'hello_world',      # name
  )
  assert qr._config.cluster() == 'smf1'
  assert qr._config.instances() == 1
  assert qr._config.name() == 'hello_world'
  assert qr._config.role() == getpass.getuser()


class MockPacker(object):
  def __init__(self, packages, package_versions, add_version):
    self.list_packages_calls, self.list_versions_calls, self.add_calls = 0, 0, 0
    self.packages = packages
    self.package_versions = package_versions
    self.add_version = add_version

  def list_packages(self, _):
    self.list_packages_calls += 1
    return self.packages

  def list_versions(self, _, name):
    self.list_versions_calls += 1
    return self.package_versions

  def add(self, _, package_name, filename, metadata, digest):
    self.add_calls += 1
    return self.add_version


def test_with_packages():
  bad_packages = (object, 23, 'derp')
  for package in bad_packages:
    with pytest.raises(ValueError):
      qr = UnpopulatedQuickrun('smf1', 'echo hello world', 'hello_world',
          packages=package)

  qr = UnpopulatedQuickrun('smf1', 'echo hello world', 'hello_world',
          packages=[('a', 'b', 23)])
  assert len(qr._config.task(0).processes().get()) == 2
  process0 = qr._config.task(0).processes()[0]
  assert process0.get() == Packer.copy(role='a', name='b', version=23).interpolate()[0].get()


def test_get_package_file_none_exists():
  # package doesn't exist
  m = mox.Mox()
  m.StubOutWithMock(sd_packer_client, 'create_packer')
  sd_packer_client.create_packer('smf1').AndReturn(MockPacker(
        [],
        [],
        {'id': 23}))
  m.ReplayAll()
  assert MockedMD5Quickrun.get_package_file('smf1', 'foo', 'bar') == (
      'foo', MockedMD5Quickrun.PACKAGE_NAME, 23)
  m.UnsetStubs()
  m.VerifyAll()

  # package exists but no versions match
  m = mox.Mox()
  m.StubOutWithMock(sd_packer_client, 'create_packer')
  sd_packer_client.create_packer('smf1').AndReturn(MockPacker(
        [MockedMD5Quickrun.PACKAGE_NAME],
        [],
        {'id': 23}))
  m.ReplayAll()
  assert MockedMD5Quickrun.get_package_file('smf1', 'foo', 'bar') == (
      'foo', MockedMD5Quickrun.PACKAGE_NAME, 23)
  m.UnsetStubs()
  m.VerifyAll()


def test_get_package_file_cached():
  m = mox.Mox()
  m.StubOutWithMock(sd_packer_client, 'create_packer')
  MockedMD5Quickrun.MD5_VALUE = 'herpderp'
  sd_packer_client.create_packer('smf1').AndReturn(MockPacker(
        [MockedMD5Quickrun.PACKAGE_NAME],
        [{'md5sum': 'foo', 'id': 15}, {'md5sum': 'herpderp', 'id': 31337}],
        {}))
  m.ReplayAll()
  try:
    assert MockedMD5Quickrun.get_package_file('smf1', 'foo', 'bar') == (
        'foo', MockedMD5Quickrun.PACKAGE_NAME, 31337)
  finally:
    MockedMD5Quickrun.MD5_VALUE = None
  m.UnsetStubs()
  m.VerifyAll()
