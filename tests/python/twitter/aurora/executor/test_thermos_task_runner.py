import contextlib
import getpass
import os
import subprocess
import sys
import tempfile
import time

from twitter.aurora.config.schema.base import (
    MB,
    MesosTaskInstance,
    Process,
    Resources,
    Task,
)
from twitter.aurora.executor.common.sandbox import DirectorySandbox
from twitter.aurora.executor.common.status_checker import ExitState
from twitter.aurora.executor.thermos_task_runner import ThermosTaskRunner
from twitter.common import log
from twitter.common.log.options import LogOptions
from twitter.common.dirutil import safe_rmtree
from twitter.common.quantity import Amount, Time
from twitter.common.contextutil import temporary_dir


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
      assert subprocess.call(["./pants", "src/python/twitter/aurora/executor:thermos_runner"]) == 0
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

      class TestThermosTaskRunner(runner_class):
        PEX_NAME = os.path.join('dist', 'thermos_runner.pex')

      task_runner = TestThermosTaskRunner(
          task_id='hello_world',
          mesos_task=TASK.bind(**bindings),
          role=getpass.getuser(),
          mesos_ports={},
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
      assert task_runner.status.status == ExitState.FINISHED

      # no-op
      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == ExitState.FINISHED

  def test_integration_failed(self):
    with self.yield_sleepy(ThermosTaskRunner, sleep=0, exit_code=1) as task_runner:
      task_runner.start()
      task_runner.forked.wait()

      self.run_to_completion(task_runner)

      assert task_runner.status is not None
      assert task_runner.status.status == ExitState.FAILED

      # no-op
      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == ExitState.FAILED

  def test_integration_stop(self):
    with self.yield_sleepy(ThermosTaskRunner, sleep=1000, exit_code=0) as task_runner:
      task_runner.start()
      task_runner.forked.wait()

      assert task_runner.status is None

      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == ExitState.KILLED

  def test_integration_lose(self):
    with self.yield_sleepy(ThermosTaskRunner, sleep=1000, exit_code=0) as task_runner:
      task_runner.start()
      task_runner.forked.wait()

      assert task_runner.status is None

      task_runner.lose()
      task_runner.stop()

      assert task_runner.status is not None
      assert task_runner.status.status == ExitState.LOST

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
      assert task_runner.status.status == ExitState.KILLED
