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
import random
import time

import mock
import pytest
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_mkdir
from twitter.common.quantity import Amount, Data
from twitter.common.recordio import ThriftRecordReader

from apache.thermos.common.path import TaskPath
from apache.thermos.core.process import LoggerMode, LogRotatingSubprocessExecutor, Process

from gen.apache.thermos.ttypes import RunnerCkpt


class TestProcess(Process):
  def execute(self):
    super(TestProcess, self).execute()
    os._exit(0)

  def finish(self):
    pass


def wait_for_rc(checkpoint, timeout=5.0):
  start = time.time()
  with open(checkpoint) as fp:
    trr = ThriftRecordReader(fp, RunnerCkpt)
    while time.time() < start + timeout:
      record = trr.read()
      if record and record.process_status and record.process_status.return_code is not None:
        return record.process_status.return_code
      else:
        time.sleep(0.1)


def get_other_nonroot_user():
  while True:
    user = random.choice(pwd.getpwall())
    if user.pw_uid not in (0, os.getuid()):
      break
  return user


def make_taskpath(td):
  return TaskPath(
      root=td,
      task_id='task',
      process='process',
      run=0,
      log_dir=os.path.join(td, '.logs'))


def setup_sandbox(td, taskpath):
  sandbox = os.path.join(td, 'sandbox')
  safe_mkdir(sandbox)
  safe_mkdir(taskpath.getpath('process_logbase'))
  safe_mkdir(os.path.dirname(taskpath.getpath('process_checkpoint')))
  return sandbox


def test_simple_process():
  with temporary_dir() as td:
    taskpath = make_taskpath(td)
    sandbox = setup_sandbox(td, taskpath)

    p = TestProcess('process', 'echo hello world', 0, taskpath, sandbox)
    p.start()
    rc = wait_for_rc(taskpath.getpath('process_checkpoint'))

    assert rc == 0
    assert_log_content(taskpath, 'stdout', 'hello world\n')


@mock.patch('os.chown')
@mock.patch('os.setgroups')
@mock.patch('os.setgid')
@mock.patch('os.setuid')
@mock.patch('os.geteuid', return_value=0)
def test_simple_process_other_user(*args):
  with temporary_dir() as td:
    some_user = get_other_nonroot_user()
    taskpath = make_taskpath(td)
    sandbox = setup_sandbox(td, taskpath)

    p = TestProcess('process', 'echo hello world', 0, taskpath, sandbox, user=some_user.pw_name)
    p.start()
    wait_for_rc(taskpath.getpath('process_checkpoint'))

    # since we're not actually root, the best we can do is check the right things were attempted
    assert os.setgroups.calledwith([g.gr_gid for g in grp.getgrall() if some_user.pw_name in g])
    assert os.setgid.calledwith(some_user.pw_uid)
    assert os.setuid.calledwith(some_user.pw_gid)


def test_other_user_fails_nonroot():
  with temporary_dir() as td:
    taskpath = make_taskpath(td)
    sandbox = setup_sandbox(td, taskpath)
    process = TestProcess(
        'process',
        'echo hello world',
        0,
        taskpath,
        sandbox,
        user=get_other_nonroot_user().pw_name)
    with pytest.raises(Process.PermissionError):
      process.start()


def test_log_permissions():
  with temporary_dir() as td:
    taskpath = make_taskpath(td)
    sandbox = setup_sandbox(td, taskpath)

    p = TestProcess('process', 'echo hello world', 0, taskpath, sandbox)
    p.start()
    wait_for_rc(taskpath.getpath('process_checkpoint'))

    stdout = taskpath.with_filename('stdout').getpath('process_logdir')
    stderr = taskpath.with_filename('stderr').getpath('process_logdir')
    assert os.path.exists(stdout)
    assert os.path.exists(stderr)
    assert os.stat(stdout).st_uid == os.getuid()
    assert os.stat(stderr).st_uid == os.getuid()


@mock.patch('os.chown')
@mock.patch('os.setgroups')
@mock.patch('os.setgid')
@mock.patch('os.setuid')
@mock.patch('os.geteuid', return_value=0)
def test_log_permissions_other_user(*mocks):
  with temporary_dir() as td:
    some_user = get_other_nonroot_user()
    taskpath = make_taskpath(td)
    sandbox = setup_sandbox(td, taskpath)

    p = TestProcess('process', 'echo hello world', 0, taskpath, sandbox, user=some_user.pw_name)
    p.start()
    wait_for_rc(taskpath.getpath('process_checkpoint'))

    # since we're not actually root, the best we can do is check the right things were attempted
    stdout = taskpath.with_filename('stdout').getpath('process_logdir')
    stderr = taskpath.with_filename('stderr').getpath('process_logdir')
    assert os.path.exists(stdout)
    assert os.path.exists(stderr)
    assert os.chown.calledwith(stdout, some_user.pw_uid, some_user.pw_gid)
    assert os.chown.calledwith(stderr, some_user.pw_uid, some_user.pw_gid)


def test_cloexec():
  def run_with_class(process_class):
    with temporary_dir() as td:
      taskpath = make_taskpath(td)
      sandbox = setup_sandbox(td, taskpath)
      with open(os.path.join(sandbox, 'silly_pants'), 'w') as silly_pants:
        p = process_class('process', 'echo test >&%s' % silly_pants.fileno(),
            0, taskpath, sandbox)
        p.start()
        return wait_for_rc(taskpath.getpath('process_checkpoint'))

  class TestWithoutCloexec(TestProcess):
    FD_CLOEXEC = False

  assert run_with_class(TestWithoutCloexec) == 0
  assert run_with_class(TestProcess) != 0


STDERR = 'for i in {1..31};do echo "stderr" 1>&2; done;'
STDOUT = 'for i in {1..31};do echo "stdout";done;'


def test_log_standard():
  with temporary_dir() as td:
    taskpath = make_taskpath(td)
    sandbox = setup_sandbox(td, taskpath)

    script = STDERR + STDOUT
    p = TestProcess('process', script, 0, taskpath, sandbox)
    p.start()

    rc = wait_for_rc(taskpath.getpath('process_checkpoint'))
    assert rc == 0
    assert_log_content(taskpath, 'stdout', 'stdout\n' * 31)
    assert_log_content(taskpath, 'stderr', 'stderr\n' * 31)


def test_log_rotation():
  # During testing, read one byte at a time to make the file sizes deterministic.
  LogRotatingSubprocessExecutor.READ_BUFFER_SIZE = 1

  def assert_stderr(taskpath, solo=True):
    if solo:
      assert_log_content(taskpath, 'stdout', '')

    assert_log_content(taskpath, 'stderr', 'stderr\n')
    assert_log_content(taskpath, 'stderr.1', 'stderr\n' * 10)
    assert_log_content(taskpath, 'stderr.2', 'stderr\n' * 10)
    assert_log_dne(taskpath, 'stderr.3')

  def assert_stdout(taskpath, solo=True):
    if solo:
      assert_log_content(taskpath, 'stderr', '')

    assert_log_content(taskpath, 'stdout', 'stdout\n')
    assert_log_content(taskpath, 'stdout.1', 'stdout\n' * 10)
    assert_log_content(taskpath, 'stdout.2', 'stdout\n' * 10)
    assert_log_dne(taskpath, 'stdout.3')

  def assert_both(taskpath):
    assert_stderr(taskpath, solo=False)
    assert_stdout(taskpath, solo=False)

  scenarios = [
    (STDERR + STDOUT, assert_both),
    (STDERR, assert_stderr),
    (STDOUT, assert_stdout)
  ]

  for script, assertion in scenarios:
    with temporary_dir() as td:
      taskpath = make_taskpath(td)
      sandbox = setup_sandbox(td, taskpath)

      p = TestProcess(
          'process',
          script,
          0,
          taskpath,
          sandbox,
          logger_mode=LoggerMode.ROTATE,
          rotate_log_size=Amount(70, Data.BYTES),
          rotate_log_backups=2)
      p.start()

      rc = wait_for_rc(taskpath.getpath('process_checkpoint'))
      assert rc == 0
      assertion(taskpath)


def assert_log_content(taskpath, log_name, expected_content):
  log = taskpath.with_filename(log_name).getpath('process_logdir')
  assert os.path.exists(log)
  with open(log, 'r') as fp:
    assert fp.read() == expected_content


def assert_log_dne(taskpath, log_name):
  log = taskpath.with_filename(log_name).getpath('process_logdir')
  assert not os.path.exists(log)


@mock.patch.dict('os.environ', values={'PATH': 'SOME_PATH', 'TEST': 'A_TEST_VAR'}, clear=True)
def test_preserve_env(*mocks):

  scenarios = [
    ('PATH', True, 'SOME_PATH'),
    ('TEST', True, 'A_TEST_VAR'),
    ('PATH', False, 'SOME_PATH'),
    ('TEST', False, ''),
  ]

  for var, preserve, expectation in scenarios:
    with temporary_dir() as td:
      taskpath = make_taskpath(td)
      sandbox = setup_sandbox(td, taskpath)

      p = TestProcess('process', 'echo $' + var, 0, taskpath, sandbox, preserve_env=preserve)
      p.start()
      rc = wait_for_rc(taskpath.getpath('process_checkpoint'))

      assert rc == 0
      assert_log_content(taskpath, 'stdout', expectation + '\n')
