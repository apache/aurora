import os
import threading
import time
import traceback

from twitter.aurora.common.http_signaler import HttpSignaler
from twitter.common import log
from twitter.common.concurrent import deadline, defer, Timeout
from twitter.common.metrics import Observable
from twitter.common.quantity import Amount, Time
from twitter.thermos.common.path import TaskPath
from twitter.thermos.monitoring.monitor import TaskMonitor

from .common.health_checker import HealthCheckerThread
from .common.kill_manager import KillManager
from .common.sandbox import DirectorySandbox, SandboxProvider
from .common.status_checker import ChainedStatusChecker, ExitState
from .common.task_info import (
    assigned_task_from_mesos_task,
    mesos_task_instance_from_assigned_task,
    resolve_ports,
)
from .common_internal.discovery_manager import DiscoveryManager
from .common_internal.resource_checkpoints import ResourceCheckpointer
from .common_internal.resource_manager import ResourceManager
from .executor_base import ThermosExecutorBase
from .executor_detector import ExecutorDetector
from .status_manager import StatusManager
from .thermos_task_runner import TaskRunner

import mesos_pb2 as mesos_pb


class DefaultSandboxProvider(SandboxProvider):
  SANDBOX_NAME = 'sandbox'

  def from_assigned_task(self, assigned_task):
    mesos_task = mesos_task_instance_from_assigned_task(assigned_task)
    return DirectorySandbox(
        os.path.realpath(self.SANDBOX_NAME),
        mesos_task.role().get())


class ThermosExecutor(Observable, ThermosExecutorBase):
  PERSISTENCE_WAIT = Amount(5, Time.SECONDS)
  SANDBOX_INITIALIZATION_TIMEOUT = Amount(10, Time.MINUTES)
  START_TIMEOUT = Amount(2, Time.MINUTES)
  STOP_TIMEOUT = Amount(2, Time.MINUTES)
  STOP_WAIT = Amount(5, Time.SECONDS)

  def __init__(self,
               runner_class,
               manager_class=StatusManager,
               sandbox_provider=DefaultSandboxProvider,
               clock=time):

    ThermosExecutorBase.__init__(self)
    if not issubclass(runner_class, TaskRunner):
      raise TypeError('runner_class must be a TaskRunner subclass, got %s' % runner_class)
    self._runner = None
    self._clock = clock
    self._task_id = None
    self._status_manager = None
    self._runner_class = runner_class
    self._manager_class = manager_class
    self._sandbox = None
    self._sandbox_provider = sandbox_provider()
    self._kill_manager = KillManager()
    # Events that are exposed for interested entities
    self.runner_aborted = threading.Event()
    self.runner_started = threading.Event()
    self.sandbox_initialized = threading.Event()
    self.sandbox_created = threading.Event()
    self.terminated = threading.Event()
    self.launched = threading.Event()

  @property
  def runner(self):
    return self._runner

  def _die(self, driver, status, msg):
    log.fatal(msg)
    self.send_update(driver, self._task_id, status, msg)
    defer(driver.stop, delay=self.STOP_WAIT)

  def _run(self, driver, assigned_task, mesos_task):
    """
      Commence running a Task.
        - Initialize the sandbox
        - Start the ThermosTaskRunner (fork the Thermos TaskRunner)
        - Set up necessary HealthCheckers
        - Set up DiscoveryManager, if applicable
        - Set up ResourceCheckpointer
        - Set up StatusManager, and attach HealthCheckers
    """
    self.send_update(driver, self._task_id, mesos_pb.TASK_STARTING, 'Initializing sandbox.')

    if not self._initialize_sandbox(driver, assigned_task):
      return

    # Fully resolve the portmap
    portmap = resolve_ports(mesos_task, assigned_task.assignedPorts)

    # start the process on a separate thread and give the message processing thread back
    # to the driver
    try:
      self._runner = self._runner_class(
          self._task_id,
          mesos_task,
          mesos_task.role().get(),
          portmap,
          self._sandbox)
    except self._runner_class.TaskError as e:
      self._die(driver, mesos_pb.TASK_FAILED, str(e))
      return

    if not self._start_runner(driver, assigned_task, mesos_task, portmap):
      return

    self.send_update(driver, self._task_id, mesos_pb.TASK_RUNNING)

    self._start_status_manager(driver, assigned_task, mesos_task, portmap)

  def _initialize_sandbox(self, driver, assigned_task):
    self._sandbox = self._sandbox_provider.from_assigned_task(assigned_task)
    self.sandbox_initialized.set()
    try:
      deadline(self._sandbox.create, timeout=self.SANDBOX_INITIALIZATION_TIMEOUT,
               daemon=True, propagate=True)
    except Timeout:
      self._die(driver, mesos_pb.TASK_FAILED, 'Timed out waiting for sandbox to initialize!')
      return
    except self._sandbox.Error as e:
      self._die(driver, mesos_pb.TASK_FAILED, 'Failed to initialize sandbox: %s' % e)
      return
    self.sandbox_created.set()
    return True

  def _start_runner(self, driver, assigned_task, mesos_task, portmap):
    if self.runner_aborted.is_set():
      self._die(driver, mesos_pb.TASK_KILLED, 'Task killed during initialization.')

    try:
      deadline(self._runner.start, timeout=self.START_TIMEOUT, propagate=True)
    except self._runner.TaskError as e:
      self._die(driver, mesos_pb.TASK_FAILED, 'Task initialization failed: %s' % e)
      return False
    except Timeout:
      self._die(driver, mesos_pb.TASK_LOST, 'Timed out waiting for task to start!')
      return False

    self.runner_started.set()
    log.debug('Task started.')

    return True

  def _start_status_manager(self, driver, assigned_task, mesos_task, portmap):
    status_checkers = [self._kill_manager]
    self.metrics.register_observable('kill_manager', self._kill_manager)

    if portmap.get('health'):
      health_check_config = mesos_task.health_check_config().get()
      http_signaler = HttpSignaler(
          portmap.get('health'),
          timeout_secs=health_check_config.get('timeout_secs'))
      health_checker = HealthCheckerThread(
          http_signaler.health,
          interval_secs=health_check_config.get('interval_secs'),
          initial_interval_secs=health_check_config.get('initial_interval_secs'),
          max_consecutive_failures=health_check_config.get('max_consecutive_failures'))
      status_checkers.append(health_checker)
      self.metrics.register_observable('health_checker', health_checker)

    task_path = TaskPath(root=self._runner._checkpoint_root, task_id=self._task_id)
    resource_manager = ResourceManager(
        mesos_task.task().resources(),
        TaskMonitor(task_path, self._task_id),
        self._sandbox.root,
    )
    status_checkers.append(resource_manager)
    self.metrics.register_observable('resource_manager', resource_manager)

    ResourceCheckpointer(
        lambda: resource_manager.sample,
        os.path.join(self._runner.artifact_dir, ExecutorDetector.RESOURCE_PATH),
        recordio=True).start()

    discovery_manager = DiscoveryManager.from_assigned_task(assigned_task)
    if discovery_manager:
      status_checkers.append(discovery_manager)
      self.metrics.register_observable('discovery_manager', discovery_manager)

    self._chained_checker = ChainedStatusChecker(status_checkers)
    self._chained_checker.start()

    # chain the runner to the other checkers, but do not chain .start()/.stop()
    complete_checker = ChainedStatusChecker([self._runner, self._chained_checker])
    self._status_manager = self._manager_class(complete_checker, self._shutdown, clock=self._clock)
    self._status_manager.start()

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

  @classmethod
  def translate_exit_state_to_mesos(cls, exit_state):
    # Put into executor_base?
    if exit_state == ExitState.FAILED:
      return mesos_pb.TASK_FAILED
    elif exit_state == ExitState.KILLED:
      return mesos_pb.TASK_KILLED
    elif exit_state == ExitState.FINISHED:
      return mesos_pb.TASK_FINISHED
    elif exit_state == ExitState.LOST:
      return mesos_pb.TASK_LOST
    log.error('Unknown exit state, defaulting to TASK_FINISHED.')
    return mesos_pb.TASK_FINISHED

  def _shutdown(self, status_result):
    runner_status = self._runner.status

    try:
      deadline(self._runner.stop, timeout=self.STOP_TIMEOUT)
    except Timeout:
      log.error('Failed to stop runner within deadline.')

    try:
      deadline(self._chained_checker.stop, timeout=self.STOP_TIMEOUT)
    except Timeout:
      log.error('Failed to stop all checkers within deadline.')

    # If the runner was alive when _shutdown was called, defer to the status_result,
    # otherwise the runner's terminal state is the preferred state.
    exit_status = runner_status or status_result

    self.send_update(
        self._driver,
        self._task_id,
        self.translate_exit_state_to_mesos(exit_status.status),
        status_result.reason)

    self.terminated.set()
    defer(self._driver.stop, delay=self.PERSISTENCE_WAIT)

  """ Mesos Executor API methods follow """

  def launchTask(self, driver, task):
    """
      Invoked when a task has been launched on this executor (initiated via Scheduler::launchTasks).
      Note that this task can be realized with a thread, a process, or some simple computation,
      however, no other callbacks will be invoked on this executor until this callback has returned.
    """
    self.launched.set()
    self.log('launchTask got task: %s:%s' % (task.name, task.task_id.value))

    # TODO(wickman)  Update the tests to call registered(), then remove this line and issue
    # an assert if self._driver is not populated.
    self._driver = driver

    if self._runner:
      log.error('Already running a task! %s' % self._task_id)
      self.send_update(driver, task.task_id.value, mesos_pb.TASK_LOST,
          "Task already running on this executor: %s" % self._task_id)
      return

    self._slave_id = task.slave_id.value
    self._task_id = task.task_id.value

    try:
      assigned_task = assigned_task_from_mesos_task(task)
      mesos_task = mesos_task_instance_from_assigned_task(assigned_task)
    except Exception as e:
      log.fatal('Could not deserialize AssignedTask')
      log.fatal(traceback.format_exc())
      self.send_update(
          driver, self._task_id, mesos_pb.TASK_FAILED, "Could not deserialize task: %s" % e)
      defer(driver.stop, delay=self.STOP_WAIT)
      return

    defer(lambda: self._run(driver, assigned_task, mesos_task))

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
