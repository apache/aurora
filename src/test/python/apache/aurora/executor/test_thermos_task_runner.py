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

import contextlib
import getpass
import os
import signal
import subprocess
import sys
import tempfile
import time

import pytest
from mesos.interface import mesos_pb2
from mock import Mock, call, patch
from twitter.common import log
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_rmtree
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time

from apache.aurora.config.schema.base import MB, MesosTaskInstance, Process, Resources, Task
from apache.aurora.executor.common.sandbox import DirectorySandbox
from apache.aurora.executor.common.status_checker import StatusResult
from apache.aurora.executor.http_lifecycle import HttpLifecycleManager
from apache.aurora.executor.thermos_task_runner import ThermosTaskRunner
from apache.thermos.common.statuses import (
    INTERNAL_ERROR,
    INVALID_TASK,
    TERMINAL_TASK,
    UNKNOWN_ERROR,
    UNKNOWN_USER
)

from gen.apache.thermos.ttypes import TaskState

TASK = MesosTaskInstance(
    instance=0,
    role=getpass.getuser(),
    task=Task(
        resources=Resources(cpu=1.0, ram=16 * MB, disk=32 * MB),
        name='hello_world',
        processes=[
            Process(name='hello_world', cmdline='{{command}}')
        ],
    )
)


class TestThermosTaskRunnerIntegration(object):
  PEX_PATH = None
  LOG_DIR = None

  @classmethod
  def setup_class(cls):
    cls.LOG_DIR = tempfile.mkdtemp()
    LogOptions.set_log_dir(cls.LOG_DIR)
    LogOptions.set_disk_log_level('DEBUG')
    log.init('executor_logger')
    if not cls.PEX_PATH:
      pex_dir = tempfile.mkdtemp()
      assert subprocess.call(["./pants", "--pants-distdir=%s" % pex_dir, "binary",
          "src/main/python/apache/thermos/runner:thermos_runner"]) == 0
      cls.PEX_PATH = os.path.join(pex_dir, 'thermos_runner.pex')

  @classmethod
  def teardown_class(cls):
    if 'THERMOS_DEBUG' not in os.environ:
      safe_rmtree(cls.LOG_DIR)
      if cls.PEX_PATH:
        safe_rmtree(os.path.dirname(cls.PEX_PATH))
    else:
      print('Saving executor logs in %s' % cls.LOG_DIR)
      if cls.PEX_PATH:
        print('Saved thermos executor at %s' % cls.PEX_PATH)

  @contextlib.contextmanager
  def yield_runner(self, runner_class, portmap=None, clock=time, preserve_env=False, **bindings):
    with contextlib.nested(temporary_dir(), temporary_dir()) as (td1, td2):
      sandbox = DirectorySandbox(td1)
      checkpoint_root = td2
      if not portmap:
        portmap = {}

      task_runner = runner_class(
          runner_pex=self.PEX_PATH,
          task_id='hello_world',
          task=TASK.bind(**bindings).task(),
          role=getpass.getuser(),
          portmap=portmap,
          clock=clock,
          sandbox=sandbox,
          checkpoint_root=checkpoint_root,
          preserve_env=preserve_env,
      )

      yield task_runner

  def yield_sleepy(self, runner_class, sleep, exit_code, portmap={}, clock=time,
          preserve_env=False):
    return self.yield_runner(
        runner_class,
        portmap=portmap,
        clock=clock,
        preserve_env=preserve_env,
        command='sleep {{__sleep}} && exit {{__exit_code}}',
        __sleep=sleep,
        __exit_code=exit_code)

  def yield_command(self, fake_runner_command, exit_state=TaskState.SUCCESS):
    class SkipWaitStartThermosTaskRunner(ThermosTaskRunner):
      def wait_start(_, timeout=None):
        pass

      def _cmdline(_):
        return ['bash', '-c', fake_runner_command]

      def task_state(_):
        return exit_state

    return self.yield_runner(SkipWaitStartThermosTaskRunner, command='ignore')

  @contextlib.contextmanager
  def exit_with_status(self, exit_code, exit_state=TaskState.SUCCESS):
    with self.yield_command('exit %d' % exit_code, exit_state=exit_state) as task_runner:
      task_runner.start()

      while task_runner.is_alive:
        task_runner._dead.wait(timeout=0.1)

      yield task_runner

  def run_to_completion(self, runner, max_wait=Amount(10, Time.SECONDS)):
    poll_interval = Amount(100, Time.MILLISECONDS)
    total_time = Amount(0, Time.SECONDS)
    while runner.status is None and total_time < max_wait:
      total_time += poll_interval
      time.sleep(poll_interval.as_(Time.SECONDS))

  def test_integration_success(self):
    with self.yield_sleepy(ThermosTaskRunner, sleep=0, exit_code=0) as task_runner:
      task_runner.start()
      task_runner.forked.wait()

      self.run_to_completion(task_runner)

      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_FINISHED

      # no-op
      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_FINISHED

  def test_integration_failed(self):
    with self.yield_sleepy(ThermosTaskRunner, sleep=0, exit_code=1) as task_runner:
      task_runner.start()
      task_runner.forked.wait()

      self.run_to_completion(task_runner)

      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_FAILED

      # no-op
      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_FAILED

  @pytest.mark.skipif('True', reason='Flaky test (AURORA-1054)')
  def test_integration_stop(self):
    with self.yield_sleepy(ThermosTaskRunner, sleep=1000, exit_code=0) as task_runner:
      task_runner.start()
      task_runner.forked.wait()

      assert task_runner.status is None

      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_KILLED

  def test_integration_lose(self):
    with self.yield_sleepy(ThermosTaskRunner, sleep=1000, exit_code=0) as task_runner:
      task_runner.start()
      task_runner.forked.wait()

      assert task_runner.status is None

      task_runner.lose()
      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_LOST

  @pytest.mark.skipif('True', reason='Flaky test (AURORA-1054)')
  def test_integration_ignores_sigterm(self):
    ignorant_script = ';'.join([
        'import time, signal',
        'signal.signal(signal.SIGTERM, signal.SIG_IGN)',
        'time.sleep(1000)'
    ])

    class ShortPreemptionThermosTaskRunner(ThermosTaskRunner):
      THERMOS_PREEMPTION_WAIT = Amount(1, Time.SECONDS)

    with self.yield_runner(
        ShortPreemptionThermosTaskRunner,
        command="%s -c '%s'" % (sys.executable, ignorant_script)) as task_runner:

      task_runner.start()
      task_runner.forked.wait()
      task_runner.stop(timeout=Amount(5, Time.SECONDS))
      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_KILLED

  @patch('apache.aurora.executor.http_lifecycle.HttpSignaler')
  def test_integration_http_teardown_killed(self, SignalerClass):
    """Ensure that the http teardown procedure closes correctly when abort kills the process."""
    signaler = SignalerClass.return_value
    signaler.side_effect = lambda path, use_post_method: (path == '/abortabortabort', None)

    clock = Mock(wraps=time)

    class TerminalStateStatusRunner(ThermosTaskRunner):
      """
      Status is called each poll in the teardown procedure. We return kill after the 3rd poll
      to mimic a task that exits early. We want to ensure the shutdown procedure doesn't wait
      the full time if it doesn't need to.
      """

      TIMES_CALLED = 0

      @property
      def status(self):
        if (self.TIMES_CALLED >= 3):
          return StatusResult('Test task mock status', mesos_pb2.TASK_KILLED)
        self.TIMES_CALLED += 1

    with self.yield_sleepy(
        TerminalStateStatusRunner,
        portmap={'health': 3141},
        clock=clock,
        sleep=0,
        exit_code=0) as task_runner:

      graceful_shutdown_wait = Amount(1, Time.SECONDS)
      shutdown_wait = Amount(5, Time.SECONDS)
      http_task_runner = HttpLifecycleManager(
          task_runner, 3141, [('/quitquitquit', graceful_shutdown_wait),
          ('/abortabortabort', shutdown_wait)], clock=clock)
      http_task_runner.start()
      task_runner.forked.wait()
      http_task_runner.stop()

      http_teardown_poll_wait_call = call(HttpLifecycleManager.WAIT_POLL_INTERVAL.as_(Time.SECONDS))
      assert clock.sleep.mock_calls.count(http_teardown_poll_wait_call) == 3  # Killed before 5
      assert signaler.mock_calls == [
        call('/quitquitquit', use_post_method=True),
        call('/abortabortabort', use_post_method=True)]

  @patch('apache.aurora.executor.http_lifecycle.HttpSignaler')
  def test_integration_http_teardown_escalate(self, SignalerClass):
    """Ensure that the http teardown process fully escalates when quit/abort both fail to kill."""
    signaler = SignalerClass.return_value
    signaler.side_effect = lambda path, use_post_method: (True, None)

    clock = Mock(wraps=time)

    class KillCalledTaskRunner(ThermosTaskRunner):
      def __init__(self, *args, **kwargs):
        self._killed_called = False
        ThermosTaskRunner.__init__(self, *args, **kwargs)

      def kill_called(self):
        return self._killed_called

      def kill(self):
        self._killed_called = True

      @property
      def status(self):
        return None

    with self.yield_sleepy(
        KillCalledTaskRunner,
        portmap={'health': 3141},
        clock=clock,
        sleep=0,
        exit_code=0) as task_runner:

      graceful_shutdown_wait = Amount(1, Time.SECONDS)
      shutdown_wait = Amount(5, Time.SECONDS)
      http_task_runner = HttpLifecycleManager(
          task_runner, 3141, [('/quitquitquit', graceful_shutdown_wait),
          ('/abortabortabort', shutdown_wait)], clock=clock)
      http_task_runner.start()
      task_runner.forked.wait()
      http_task_runner.stop()

      http_teardown_poll_wait_call = call(HttpLifecycleManager.WAIT_POLL_INTERVAL.as_(Time.SECONDS))
      assert clock.sleep.mock_calls.count(http_teardown_poll_wait_call) == 6
      assert signaler.mock_calls == [
        call('/quitquitquit', use_post_method=True),
        call('/abortabortabort', use_post_method=True)]
      assert task_runner.kill_called() is True

  def test_thermos_normal_exit_status(self):
    with self.exit_with_status(0, TaskState.SUCCESS) as task_runner:
      assert task_runner._popen_signal == 0
      assert task_runner._popen_rc == 0
      status = task_runner.compute_status()
      assert status.reason == 'Task finished.'
      assert status.status is mesos_pb2.TASK_FINISHED

    with self.exit_with_status(0, None) as task_runner:
      assert task_runner._popen_signal == 0
      assert task_runner._popen_rc == 0
      status = task_runner.compute_status()
      assert status.status is mesos_pb2.TASK_LOST

  def test_thermos_abnormal_exit_statuses(self):
    with self.exit_with_status(UNKNOWN_USER) as task_runner:
      assert task_runner._popen_signal == 0
      assert task_runner._popen_rc == UNKNOWN_USER
      status = task_runner.compute_status()
      assert 'unknown user' in status.reason
      assert status.status is mesos_pb2.TASK_FAILED

    with self.exit_with_status(INTERNAL_ERROR) as task_runner:
      assert task_runner._popen_signal == 0
      assert task_runner._popen_rc == INTERNAL_ERROR
      status = task_runner.compute_status()
      assert 'internal error' in status.reason
      assert status.status is mesos_pb2.TASK_LOST

    with self.exit_with_status(INVALID_TASK) as task_runner:
      assert task_runner._popen_signal == 0
      assert task_runner._popen_rc == INVALID_TASK
      status = task_runner.compute_status()
      assert 'invalid task' in status.reason
      assert status.status is mesos_pb2.TASK_FAILED

    with self.exit_with_status(TERMINAL_TASK, exit_state=TaskState.FAILED) as task_runner:
      assert task_runner._popen_signal == 0
      assert task_runner._popen_rc == TERMINAL_TASK
      status = task_runner.compute_status()
      assert 'Task failed' in status.reason
      assert status.status is mesos_pb2.TASK_FAILED

    with self.exit_with_status(UNKNOWN_ERROR) as task_runner:
      assert task_runner._popen_signal == 0
      assert task_runner._popen_rc == UNKNOWN_ERROR
      status = task_runner.compute_status()
      assert 'unknown error' in status.reason
      assert status.status is mesos_pb2.TASK_LOST

    with self.exit_with_status(13) as task_runner:
      assert task_runner._popen_signal == 0
      assert task_runner._popen_rc == 13
      status = task_runner.compute_status()
      assert 'unknown reason' in status.reason
      assert 'exit status: 13' in status.reason
      assert status.status is mesos_pb2.TASK_LOST

  def test_thermos_runner_killed(self):
    with self.yield_command('sleep 1000') as task_runner:
      task_runner.start()

      os.kill(task_runner._popen.pid, signal.SIGKILL)

      while task_runner.is_alive:
        task_runner._dead.wait(timeout=0.1)

      assert task_runner._popen_signal == 9
      assert task_runner._popen_rc == 0  # ??
      status = task_runner.compute_status()
      assert 'killed by signal 9' in status.reason
      assert status.status is mesos_pb2.TASK_KILLED

  def test_thermos_preserve_env(self):
    with self.yield_sleepy(
        ThermosTaskRunner,
        preserve_env=True,
        sleep=0,
        exit_code=0) as task_runner:

      task_runner.start()
      task_runner.forked.wait()

      self.run_to_completion(task_runner)

      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_FINISHED

      # no-op
      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == mesos_pb2.TASK_FINISHED
