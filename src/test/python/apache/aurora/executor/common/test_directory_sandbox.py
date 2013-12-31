import os

from apache.aurora.executor.common.sandbox import DirectorySandbox
from twitter.common.contextutil import temporary_dir

import mock


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
