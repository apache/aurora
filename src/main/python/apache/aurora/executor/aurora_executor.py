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

import threading
import time
import traceback

from mesos.interface import mesos_pb2
from twitter.common import log
from twitter.common.concurrent import Timeout, deadline, defer
from twitter.common.metrics import Observable
from twitter.common.quantity import Amount, Time

from .common.kill_manager import KillManager
from .common.sandbox import DefaultSandboxProvider
from .common.status_checker import ChainedStatusChecker
from .common.task_info import assigned_task_from_mesos_task
from .common.task_runner import TaskError, TaskRunner, TaskRunnerProvider
from .executor_base import ExecutorBase
from .status_manager import StatusManager


def propagate_deadline(*args, **kw):
  return deadline(*args, daemon=True, propagate=True, **kw)


class AuroraExecutor(ExecutorBase, Observable):
  PERSISTENCE_WAIT = Amount(5, Time.SECONDS)
  SANDBOX_INITIALIZATION_TIMEOUT = Amount(10, Time.MINUTES)
  START_TIMEOUT = Amount(2, Time.MINUTES)
  STOP_TIMEOUT = Amount(2, Time.MINUTES)
  STOP_WAIT = Amount(5, Time.SECONDS)

  def __init__(
      self,
      runner_provider,
      status_manager_class=StatusManager,
      sandbox_provider=DefaultSandboxProvider(),
      status_providers=(),
      clock=time,
      no_sandbox_create_user=False,
      sandbox_mount_point=None):

    ExecutorBase.__init__(self)
    if not isinstance(runner_provider, TaskRunnerProvider):
      raise TypeError('runner_provider must be a TaskRunnerProvider, got %s' %
          type(runner_provider))
    self._runner = None
    self._runner_provider = runner_provider
    self._clock = clock
    self._task_id = None
    self._status_providers = status_providers
    self._status_manager = None
    self._status_manager_class = status_manager_class
    self._sandbox = None
    self._sandbox_provider = sandbox_provider
    self._no_sandbox_create_user = no_sandbox_create_user
    self._sandbox_mount_point = sandbox_mount_point
    self._kill_manager = KillManager()
    # Events that are exposed for interested entities
    self.runner_aborted = threading.Event()
    self.runner_started = threading.Event()
    self.sandbox_initialized = threading.Event()
    self.sandbox_created = threading.Event()
    self.status_manager_started = threading.Event()
    self.terminated = threading.Event()
    self.launched = threading.Event()

  @property
  def runner(self):
    return self._runner

  def _die(self, driver, status, msg):
    log.fatal(msg)
    self.send_update(driver, self._task_id, status, msg)
    defer(driver.stop, delay=self.STOP_WAIT)

  def _run(self, driver, assigned_task, mounted_volume_paths):
    """
      Commence running a Task.
        - Initialize the sandbox
        - Start the ThermosTaskRunner (fork the Thermos TaskRunner)
        - Set up necessary HealthCheckers
        - Set up StatusManager, and attach HealthCheckers
    """
    self.send_update(driver, self._task_id, mesos_pb2.TASK_STARTING, 'Initializing sandbox.')

    if not self._initialize_sandbox(driver, assigned_task, mounted_volume_paths):
      return

    # start the process on a separate thread and give the message processing thread back
    # to the driver
    try:
      self._runner = self._runner_provider.from_assigned_task(assigned_task, self._sandbox)
    except TaskError as e:
      self.runner_aborted.set()
      self._die(driver, mesos_pb2.TASK_FAILED, str(e))
      return

    if not isinstance(self._runner, TaskRunner):
      self._die(driver, mesos_pb2.TASK_FAILED, 'Unrecognized task!')
      return

    if not self._start_runner(driver, assigned_task):
      return

    try:
      self._start_status_manager(driver, assigned_task)
    except Exception:
      log.error(traceback.format_exc())
      self._die(driver, mesos_pb2.TASK_FAILED, "Internal error")

  def _initialize_sandbox(self, driver, assigned_task, mounted_volume_paths):
    self._sandbox = self._sandbox_provider.from_assigned_task(
        assigned_task,
        no_create_user=self._no_sandbox_create_user,
        mounted_volume_paths=mounted_volume_paths,
        sandbox_mount_point=self._sandbox_mount_point)
    self.sandbox_initialized.set()
    try:
      propagate_deadline(self._sandbox.create, timeout=self.SANDBOX_INITIALIZATION_TIMEOUT)
    except Timeout:
      self._die(driver, mesos_pb2.TASK_FAILED, 'Timed out waiting for sandbox to initialize!')
      return
    except self._sandbox.Error as e:
      self._die(driver, mesos_pb2.TASK_FAILED, 'Failed to initialize sandbox: %s' % e)
      return
    except Exception as e:
      self._die(driver, mesos_pb2.TASK_FAILED, 'Unknown exception initializing sandbox: %s' % e)
      return
    self.sandbox_created.set()
    return True

  def _start_runner(self, driver, assigned_task):
    if self.runner_aborted.is_set():
      self._die(driver, mesos_pb2.TASK_KILLED, 'Task killed during initialization.')

    try:
      propagate_deadline(self._runner.start, timeout=self.START_TIMEOUT)
    except TaskError as e:
      self._die(driver, mesos_pb2.TASK_FAILED, 'Task initialization failed: %s' % e)
      return False
    except Timeout:
      self._die(driver, mesos_pb2.TASK_LOST, 'Timed out waiting for task to start!')
      return False

    self.runner_started.set()
    log.debug('Task started.')

    return True

  def _start_status_manager(self, driver, assigned_task):
    status_checkers = [self._kill_manager]
    self.metrics.register_observable(self._kill_manager.name(), self._kill_manager)

    for status_provider in self._status_providers:
      status_checker = status_provider.from_assigned_task(assigned_task, self._sandbox)
      if status_checker is None:
        continue
      status_checkers.append(status_checker)
      self.metrics.register_observable(status_checker.name(), status_checker)

    self._chained_checker = ChainedStatusChecker(status_checkers)
    self._chained_checker.start()

    # chain the runner to the other checkers, but do not chain .start()/.stop()
    complete_checker = ChainedStatusChecker([self._runner, self._chained_checker])
    self._status_manager = self._status_manager_class(
        complete_checker,
        self._signal_running,
        self._shutdown,
        clock=self._clock)
    self._status_manager.start()
    self.status_manager_started.set()

  def _signal_running(self, status_result):
    log.info('Send TASK_RUNNING status update. status: %s' % status_result)
    self.send_update(self._driver, self._task_id, mesos_pb2.TASK_RUNNING, status_result.reason)

  def _signal_kill_manager(self, driver, task_id, reason):
    if self._task_id is None:
      log.error('Was asked to kill task but no task running!')
      return
    if task_id != self._task_id:
      log.error('Asked to kill a task other than what we are running!')
      return
    if not self.sandbox_created.is_set():
      log.error('Asked to kill task with incomplete sandbox - aborting runner start')
      self.runner_aborted.set()
      return
    self.log('Activating kill manager.')
    self._kill_manager.kill(reason)

  def _shutdown(self, status_result):
    runner_status = self._runner.status

    try:
      propagate_deadline(self._chained_checker.stop, timeout=self.STOP_TIMEOUT)
    except Timeout:
      log.error('Failed to stop all checkers within deadline.')
    except Exception:
      log.error('Failed to stop health checkers:')
      log.error(traceback.format_exc())

    try:
      propagate_deadline(self._runner.stop, timeout=self.STOP_TIMEOUT)
    except Timeout:
      log.error('Failed to stop runner within deadline.')
    except Exception:
      log.error('Failed to stop runner:')
      log.error(traceback.format_exc())

    # If the runner was alive when _shutdown was called, defer to the status_result,
    # otherwise the runner's terminal state is the preferred state.
    exit_status = runner_status or status_result

    self.send_update(
        self._driver,
        self._task_id,
        exit_status.status,
        status_result.reason)

    self.terminated.set()
    defer(self._driver.stop, delay=self.PERSISTENCE_WAIT)

  @classmethod
  def validate_task(cls, task):
    try:
      assigned_task = assigned_task_from_mesos_task(task)
      return assigned_task
    except Exception:
      log.fatal('Could not deserialize AssignedTask')
      log.fatal(traceback.format_exc())
      return None

  @classmethod
  def extract_mount_paths_from_task(cls, task):
    if task.executor and task.executor.container:
      return [v.container_path for v in task.executor.container.volumes]

    return None

  """ Mesos Executor API methods follow """

  def launchTask(self, driver, task):
    """
      Invoked when a task has been launched on this executor (initiated via Scheduler::launchTasks).
      Note that this task can be realized with a thread, a process, or some simple computation,
      however, no other callbacks will be invoked on this executor until this callback has returned.
    """
    self.launched.set()
    self.log('TaskInfo: %s' % task)
    self.log('launchTask got task: %s:%s' % (task.name, task.task_id.value))

    # TODO(wickman)  Update the tests to call registered(), then remove this line and issue
    # an assert if self._driver is not populated.
    self._driver = driver

    if self._runner:
      log.error('Already running a task! %s' % self._task_id)
      self.send_update(driver, task.task_id.value, mesos_pb2.TASK_LOST,
          "Task already running on this executor: %s" % self._task_id)
      return

    self._slave_id = task.slave_id.value
    self._task_id = task.task_id.value

    assigned_task = self.validate_task(task)
    self.log("Assigned task: %s" % assigned_task)
    if not assigned_task:
      self.send_update(driver, self._task_id, mesos_pb2.TASK_FAILED,
          'Could not deserialize task.')
      defer(driver.stop, delay=self.STOP_WAIT)
      return

    defer(lambda: self._run(driver, assigned_task, self.extract_mount_paths_from_task(task)))

  def killTask(self, driver, task_id):
    """
     Invoked when a task running within this executor has been killed (via
     SchedulerDriver::killTask). Note that no status update will be sent on behalf of the executor,
     the executor is responsible for creating a new TaskStatus (i.e., with TASK_KILLED) and invoking
     ExecutorDriver::sendStatusUpdate.
    """
    self.log('killTask got task_id: %s' % task_id)
    self._signal_kill_manager(driver, task_id.value, "Instructed to kill task.")
    self.log('killTask returned.')

  def shutdown(self, driver):
    """
     Invoked when the executor should terminate all of its currently running tasks. Note that after
     Mesos has determined that an executor has terminated any tasks that the executor did not send
     terminal status updates for (e.g., TASK_KILLED, TASK_FINISHED, TASK_FAILED, etc) a TASK_LOST
     status update will be created.

    """
    self.log('shutdown called')
    if self._task_id:
      self.log('shutting down %s' % self._task_id)
      self._signal_kill_manager(driver, self._task_id, "Told to shut down executor.")
    self.log('shutdown returned')
