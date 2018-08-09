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

import grp
import os
import pwd
import subprocess

import mock
import pytest
from twitter.common.contextutil import temporary_dir

from apache.aurora.executor.common.sandbox import (
    DefaultSandboxProvider,
    DirectorySandbox,
    DockerDirectorySandbox,
    FileSystemImageSandbox
)

from gen.apache.aurora.api.ttypes import AssignedTask, Container, DockerContainer, TaskConfig


def test_sandbox_root_path():
  with temporary_dir() as d:
    mesos_dir = os.path.join(d, 'task1')
    ds = DirectorySandbox(mesos_dir)

    assert os.path.join(mesos_dir, DirectorySandbox.SANDBOX_NAME) == ds.root


def test_directory_sandbox():
  with temporary_dir() as d:
    ds1 = DirectorySandbox(os.path.join(d, 'task1'))
    ds2 = DirectorySandbox(os.path.join(d, 'task2'))
    ds1.create()
    ds2.create()
    assert os.path.exists(ds1.root)
    assert os.path.exists(ds2.root)
    ds1.destroy()
    assert not os.path.exists(ds1.root)
    assert os.path.exists(ds2.root)
    ds2.destroy()
    assert not os.path.exists(ds2.root)


@mock.patch('grp.getgrgid')
@mock.patch('pwd.getpwnam')
@mock.patch('os.chown')
@mock.patch('os.chmod')
def test_create(chmod, chown, getpwnam, getgrgid):
  getgrgid.return_value.gr_name = 'foo'
  getpwnam.return_value.pw_gid = 123
  getpwnam.return_value.pw_uid = 456

  with temporary_dir() as mesos_dir:
    ds = DirectorySandbox(mesos_dir, 'cletus')
    ds.create()
    assert os.path.exists(ds.root)

  getpwnam.assert_called_with('cletus')
  getgrgid.assert_called_with(123)

  chown.assert_called_with(ds.root, 456, 123)
  chmod.assert_called_with(ds.root, 0700)


@mock.patch('grp.getgrgid')
@mock.patch('pwd.getpwnam')
@mock.patch('os.chown')
@mock.patch('os.chmod')
def test_create_no_user(*args):
  with temporary_dir() as d:
    ds = DirectorySandbox(d)
    ds.create()
    assert os.path.exists(ds.root)

  for mocked in args:
    mocked.assert_not_called()


@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': '/some/path'})
def test_sandbox_provider_docker_container():
  sandbox = DefaultSandboxProvider().from_assigned_task(
          AssignedTask(task=TaskConfig(container=Container(docker=DockerContainer()))))

  assert sandbox._user is None


@mock.patch('pwd.getpwnam')
def test_user_does_not_exist(getpwnam):
  getpwnam.side_effect = KeyError('johndoe')

  with temporary_dir() as d:
    ds = DirectorySandbox(d, 'cletus')
    with pytest.raises(DirectorySandbox.CreationError):
      ds.create()

  getpwnam.assert_called_with('cletus')


@mock.patch('os.chown')
def test_create_ioerror(chown):
  chown.side_effect = IOError('Disk is borked')

  with temporary_dir() as d:
    ds = DirectorySandbox(d)
    with pytest.raises(DirectorySandbox.CreationError):
      ds.create()


@mock.patch('os.makedirs')
def test_docker_directory_sandbox_create_ioerror(makedirs):
  makedirs.side_effect = IOError('Disk is borked')

  with mock.patch.dict('os.environ', {
    'MESOS_DIRECTORY': 'some-directory',
    'MESOS_SANDBOX': 'some-sandbox'
  }):
    with temporary_dir() as d:
      ds = DockerDirectorySandbox(d)
      with pytest.raises(DirectorySandbox.CreationError):
        ds.create()


def test_destroy_ioerror():
  with temporary_dir() as d:
    ds = DirectorySandbox(d)
    ds.create()

    with mock.patch('shutil.rmtree') as shutil_rmtree:
      shutil_rmtree.side_effect = IOError('What even are you doing?')
      with pytest.raises(DirectorySandbox.DeletionError):
        ds.destroy()


MOCK_MESOS_DIRECTORY = '/some/path'


@mock.patch('subprocess.check_output')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_verify_group_match(mock_check_output):
  with temporary_dir() as d:
    sandbox = FileSystemImageSandbox(d, user='someuser', sandbox_mount_point='/some/path')

    mock_check_output.return_value = 'test-group:x:2:'

    # valid case
    sandbox._verify_group_match_in_taskfs(2, 'test-group')
    mock_check_output.assert_called_with(
        ['chroot', sandbox._task_fs_root, 'getent', 'group', 'test-group'])

    # invalid group id
    with pytest.raises(FileSystemImageSandbox.CreationError):
      sandbox._verify_group_match_in_taskfs(3, 'test-group')

    # invalid group name
    with pytest.raises(FileSystemImageSandbox.CreationError):
      sandbox._verify_group_match_in_taskfs(2, 'invalid-group')

    # exception case
    exception = subprocess.CalledProcessError(
        returncode=1,
        cmd='some command',
        output=None)
    mock_check_output.side_effect = exception
    with pytest.raises(FileSystemImageSandbox.CreationError):
      sandbox._verify_group_match_in_taskfs(2, 'test-group')


def test_verify_network_files():
  with temporary_dir() as d:
    task_fs_path = os.path.join(d, 'taskfs')
    os.makedirs(os.path.join(task_fs_path, 'etc'))

    with mock.patch.dict(os.environ, {'MESOS_DIRECTORY': d}):
      sandbox = FileSystemImageSandbox(d, sandbox_mount_point='/some/sandbox/path')

      sandbox._copy_files()

    def verify_copy(path):
      if os.path.exists(path):
        assert os.path.exists(os.path.join(task_fs_path, path.lstrip('/')))

    verify_copy('/etc/resolv.conf')
    verify_copy('/etc/hostname')
    verify_copy('/etc/hosts')


@mock.patch('subprocess.check_output')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_verify_user_match(mock_check_output):
  with temporary_dir() as d:
    sandbox = FileSystemImageSandbox(
        os.path.join(d, 'sandbox'),
        user='someuser',
        sandbox_mount_point='/some/path')

    mock_check_output.return_value = 'uid=1(test-user) gid=2(test-group) groups=2(test-group)'
    # valid case
    sandbox._verify_user_match_in_taskfs(1, 'test-user', 2, 'test-group')
    mock_check_output.assert_called_with(['chroot', sandbox._task_fs_root, 'id', 'test-user'])

    # invalid user id
    with pytest.raises(FileSystemImageSandbox.CreationError):
      sandbox._verify_user_match_in_taskfs(0, 'test-user', 2, 'test-group')

    # invalid user name
    with pytest.raises(FileSystemImageSandbox.CreationError):
      sandbox._verify_user_match_in_taskfs(1, 'invalid-user', 2, 'test-group')

    # invalid group id
    with pytest.raises(FileSystemImageSandbox.CreationError):
      sandbox._verify_user_match_in_taskfs(1, 'test-user', 0, 'test-group')

    # invalid group name
    with pytest.raises(FileSystemImageSandbox.CreationError):
      sandbox._verify_user_match_in_taskfs(1, 'test-user', 2, 'invalid-group')

    # exception case
    exception = subprocess.CalledProcessError(
        returncode=1,
        cmd='some command',
        output=None)
    mock_check_output.side_effect = exception
    with pytest.raises(FileSystemImageSandbox.CreationError):
      sandbox._verify_user_match_in_taskfs(1, "test-user", 2, "test-group")


def assert_create_user_and_group(mock_check_call,
                                 mock_verify,
                                 gid_exists,
                                 uid_exists,
                                 group_name_exists,
                                 user_name_exists):
  mock_pwent = pwd.struct_passwd((
    'someuser',        # login name
    'hunter2',         # password
    834,               # uid
    835,               # gid
    'Some User',       # user name
    '/home/someuser',  # home directory
    '/bin/sh'))        # login shell

  mock_grent = grp.struct_group((
    'users',        # group name
    '*',            # password
    835,            # gid
    ['someuser']))  # members

  returncode = 0
  if gid_exists or uid_exists:
    returncode = FileSystemImageSandbox._USER_OR_GROUP_ID_EXISTS
  elif group_name_exists or user_name_exists:
    returncode = FileSystemImageSandbox._USER_OR_GROUP_NAME_EXISTS

  exception = subprocess.CalledProcessError(
      returncode=returncode,
      cmd='some command',
      output=None)

  mock_check_call.side_effect = [
      exception if gid_exists or group_name_exists else None,
      exception if uid_exists or user_name_exists else None]

  with temporary_dir() as d:
    with mock.patch.object(
            FileSystemImageSandbox,
            'get_user_and_group',
            return_value=(mock_pwent, mock_grent)):

      sandbox = FileSystemImageSandbox(
          os.path.join(d, 'sandbox'),
          user='someuser',
          sandbox_mount_point='/some/path')
      sandbox._create_user_and_group_in_taskfs()

  assert len(mock_check_call.mock_calls) == 2
  assert len(mock_verify.mock_calls) == 1


@mock.patch('apache.aurora.executor.common.sandbox.' +
            'FileSystemImageSandbox._verify_user_match_in_taskfs')
@mock.patch('subprocess.check_call')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_uid_exists(mock_check_call, mock_verify):
  assert_create_user_and_group(mock_check_call, mock_verify, False, True, False, False)


@mock.patch('apache.aurora.executor.common.sandbox.' +
            'FileSystemImageSandbox._verify_group_match_in_taskfs')
@mock.patch('subprocess.check_call')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_gid_exists(mock_check_call, mock_verify):
  assert_create_user_and_group(mock_check_call, mock_verify, True, False, False, False)


@mock.patch('apache.aurora.executor.common.sandbox.' +
            'FileSystemImageSandbox._verify_user_match_in_taskfs')
@mock.patch('subprocess.check_call')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_user_name_exists(mock_check_call, mock_verify):
  assert_create_user_and_group(mock_check_call, mock_verify, False, False, False, True)


@mock.patch('apache.aurora.executor.common.sandbox.' +
            'FileSystemImageSandbox._verify_group_match_in_taskfs')
@mock.patch('subprocess.check_call')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_group_name_exists(mock_check_call, mock_verify):
  assert_create_user_and_group(mock_check_call, mock_verify, True, False, True, False)


@mock.patch('os.path.isfile')
@mock.patch('subprocess.check_call')
@mock.patch('apache.aurora.executor.common.sandbox.safe_mkdir')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_filesystem_sandbox_mounts_paths(mock_safe_mkdir, mock_check_call, mock_isfile):
  sandbox_mount_point = '/some/mount/point'
  sandbox_directory = os.path.join(MOCK_MESOS_DIRECTORY, 'sandbox')

  sandbox = FileSystemImageSandbox(
      MOCK_MESOS_DIRECTORY,
      user='someuser',
      no_create_user=True,
      mounted_volume_paths=['/some/container/path', '/some/other/container/path'],
      sandbox_mount_point=sandbox_mount_point)

  mock_isfile.return_value = False

  sandbox._mount_paths()

  task_fs_path = os.path.join(MOCK_MESOS_DIRECTORY, 'taskfs')
  # we should have mounted both of the paths we passed in as well as the sandbox directory itself.
  assert mock_check_call.mock_calls == [
      mock.call([
          'mount',
          '-n',
          '--rbind',
          '/some/container/path',
          os.path.join(task_fs_path, 'some/container/path')
      ]),
      mock.call([
          'mount',
          '-n',
          '--rbind',
        '/some/other/container/path',
        os.path.join(task_fs_path, 'some/other/container/path')
      ]),
      mock.call([
          'mount',
          '-n',
          '--rbind',
          sandbox_directory,
          os.path.join(task_fs_path, sandbox_mount_point[1:])
      ])
  ]


@mock.patch('os.path.isfile')
@mock.patch('subprocess.check_call')
@mock.patch('apache.aurora.executor.common.sandbox.safe_mkdir')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_filesystem_sandbox_no_volumes(mock_safe_mkdir, mock_check_call, mock_isfile):
  sandbox_mount_point = '/some/mount/point'
  sandbox_directory = os.path.join(MOCK_MESOS_DIRECTORY, 'sandbox')

  sandbox = FileSystemImageSandbox(
      MOCK_MESOS_DIRECTORY,
      user='someuser',
      no_create_user=True,
      mounted_volume_paths=None,
      sandbox_mount_point=sandbox_mount_point)

  mock_isfile.return_value = False

  sandbox._mount_paths()

  task_fs_path = os.path.join(MOCK_MESOS_DIRECTORY, 'taskfs')

  assert mock_check_call.mock_calls == [
    mock.call([
      'mount',
      '-n',
      '--rbind',
      sandbox_directory,
      os.path.join(task_fs_path, sandbox_mount_point[1:])
    ])
  ]


@mock.patch('apache.aurora.executor.common.sandbox.touch')
@mock.patch('os.path.exists')
@mock.patch('os.path.isfile')
@mock.patch('subprocess.check_call')
@mock.patch('apache.aurora.executor.common.sandbox.safe_mkdir')
@mock.patch.dict(os.environ, {'MESOS_DIRECTORY': MOCK_MESOS_DIRECTORY})
def test_filesystem_sandbox_mounts_paths_source_is_file(
    mock_safe_mkdir,
    mock_check_call,
    mock_isfile,
    mock_exists,
    mock_touch):

  sandbox_mount_point = '/some/mount/point'
  sandbox_directory = os.path.join(MOCK_MESOS_DIRECTORY, 'sandbox')

  sandbox = FileSystemImageSandbox(
      MOCK_MESOS_DIRECTORY,
      user='someuser',
      no_create_user=True,
      mounted_volume_paths=['/some/path/to/file', '/some/path/to/directory'],
      sandbox_mount_point=sandbox_mount_point)

  def is_file_side_effect(arg):
    return arg.endswith('file')

  mock_isfile.side_effect = is_file_side_effect
  mock_exists.return_value = False

  sandbox._mount_paths()

  task_fs_path = os.path.join(MOCK_MESOS_DIRECTORY, 'taskfs')
  destination_path = os.path.join(task_fs_path, 'some/path/to/file')

  # we should `touch` the mount point, but only for the file mount, not the directory mount.
  assert mock_touch.mock_calls == [
    mock.call(destination_path)
  ]

  # we should have mounted the file path we passed in as well as the sandbox directory itself.
  assert mock_check_call.mock_calls == [
    mock.call([
      'mount',
      '-n',
      '--rbind',
      '/some/path/to/file',
      destination_path
    ]),
    mock.call([
      'mount',
      '-n',
      '--rbind',
      '/some/path/to/directory',
      os.path.join(task_fs_path, 'some/path/to/directory')
    ]),
    mock.call([
      'mount',
      '-n',
      '--rbind',
      sandbox_directory,
      os.path.join(task_fs_path, sandbox_mount_point[1:])
    ])
  ]
