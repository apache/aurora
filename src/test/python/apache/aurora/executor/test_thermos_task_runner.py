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

from mesos.interface import mesos_pb2
from twitter.common import log
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_rmtree
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time

from apache.aurora.config.schema.base import MB, MesosTaskInstance, Process, Resources, Task
from apache.aurora.executor.common.sandbox import DirectorySandbox
from apache.aurora.executor.thermos_statuses import (
    INTERNAL_ERROR,
    INVALID_TASK,
    TERMINAL_TASK,
    UNKNOWN_ERROR,
    UNKNOWN_USER
)
from apache.aurora.executor.thermos_task_runner import ThermosTaskRunner

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
  PANTS_BUILT = False
  LOG_DIR = None

  @classmethod
  def setup_class(cls):
    cls.LOG_DIR = tempfile.mkdtemp()
    LogOptions.set_log_dir(cls.LOG_DIR)
    LogOptions.set_disk_log_level('DEBUG')
    log.init('executor_logger')
    if not cls.PANTS_BUILT and 'SKIP_PANTS_BUILD' not in os.environ:
      assert subprocess.call(["./pants",
          "src/main/python/apache/aurora/executor/bin:thermos_runner"]) == 0
      cls.PANTS_BUILT = True

  @classmethod
  def teardown_class(cls):
    if 'THERMOS_DEBUG' not in os.environ:
      safe_rmtree(cls.LOG_DIR)
    else:
      print('Saving executor logs in %s' % cls.LOG_DIR)

  @contextlib.contextmanager
  def yield_runner(self, runner_class, **bindings):
    with contextlib.nested(temporary_dir(), temporary_dir()) as (td1, td2):
      sandbox = DirectorySandbox(td1)
      checkpoint_root = td2

      task_runner = runner_class(
          runner_pex=os.path.join('dist', 'thermos_runner.pex'),
          task_id='hello_world',
          task=TASK.bind(**bindings).task(),
          role=getpass.getuser(),
          portmap={},
          sandbox=sandbox,
          checkpoint_root=checkpoint_root,
      )

      yield task_runner

  def yield_sleepy(self, runner_class, sleep, exit_code):
    return self.yield_runner(
        runner_class,
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

  def test_integration_quitquitquit(self):
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
