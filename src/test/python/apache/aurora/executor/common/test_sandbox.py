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

import mock
import pytest
from twitter.common.contextutil import temporary_dir

from apache.aurora.executor.common.sandbox import (
    DefaultSandboxProvider,
    DirectorySandbox,
    FileSystemImageSandbox
)

from gen.apache.aurora.api.ttypes import AssignedTask, Container, DockerContainer, TaskConfig


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

  with temporary_dir() as d:
    real_path = os.path.join(d, 'sandbox')
    ds = DirectorySandbox(real_path, 'cletus')
    ds.create()
    assert os.path.exists(real_path)

  getpwnam.assert_called_with('cletus')
  getgrgid.assert_called_with(123)
  chown.assert_called_with(real_path, 456, 123)
  chmod.assert_called_with(real_path, 0700)


@mock.patch('grp.getgrgid')
@mock.patch('pwd.getpwnam')
@mock.patch('os.chown')
@mock.patch('os.chmod')
def test_create_no_user(*args):
  with temporary_dir() as d:
    real_path = os.path.join(d, 'sandbox')
    ds = DirectorySandbox(real_path)
    ds.create()
    assert os.path.exists(real_path)

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
    real_path = os.path.join(d, 'sandbox')
    ds = DirectorySandbox(real_path, 'cletus')
    with pytest.raises(DirectorySandbox.CreationError):
      ds.create()

  getpwnam.assert_called_with('cletus')


@mock.patch('os.chown')
def test_create_ioerror(chown):
  chown.side_effect = IOError('Disk is borked')

  with temporary_dir() as d:
    real_path = os.path.join(d, 'sandbox')
    ds = DirectorySandbox(real_path)
    with pytest.raises(DirectorySandbox.CreationError):
      ds.create()


@mock.patch('os.makedirs')
def test_filesystem_image_sandbox_create_ioerror(makedirs):
  makedirs.side_effect = IOError('Disk is borked')

  with mock.patch.dict('os.environ', {
    FileSystemImageSandbox.MESOS_DIRECTORY_ENV_VARIABLE: 'some-directory',
    FileSystemImageSandbox.MESOS_SANDBOX_ENV_VARIABLE: 'some-sandbox'
  }):
    with temporary_dir() as d:
      real_path = os.path.join(d, 'sandbox')
      ds = FileSystemImageSandbox(real_path)
      with pytest.raises(DirectorySandbox.CreationError):
        ds.create()


def test_destroy_ioerror():
  with temporary_dir() as d:
    real_path = os.path.join(d, 'sandbox')
    ds = DirectorySandbox(real_path)
    ds.create()

    with mock.patch('shutil.rmtree') as shutil_rmtree:
      shutil_rmtree.side_effect = IOError('What even are you doing?')
      with pytest.raises(DirectorySandbox.DeletionError):
        ds.destroy()
