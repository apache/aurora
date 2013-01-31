import json
import os
import socket
import sys
import threading
import time

# mesos
import mesos_pb2 as mesos_pb

from twitter.common import log
from twitter.common.concurrent import deadline, defer, Timeout
from twitter.common.exceptions import ExceptionalThread
from twitter.common.quantity import Amount, Time

# thermos
from twitter.mesos.config.schema import MesosTaskInstance
from twitter.mesos.executor.task_runner_wrapper import (
  ProductionTaskRunner,
  AngrybirdTaskRunner)
from twitter.mesos.executor.executor_base import ThermosExecutorBase
from twitter.mesos.parsers.base import PortResolver

from twitter.thermos.base.path import TaskPath
from twitter.thermos.monitoring.monitor import TaskMonitor

# thrifts
from gen.twitter.mesos.ttypes import AssignedTask
from thrift.TSerialization import deserialize as thrift_deserialize

from .discovery_manager import DiscoveryManager
from .health_checker import HealthCheckerThread
from .http_signaler import HttpSignaler
from .kill_manager import KillManager
from .resource_checkpoints import ResourceCheckpointer
from .resource_manager import ResourceManager
from .status_manager import StatusManager


if 'ANGRYBIRD_THERMOS' in os.environ:
  RUNNER_CLASS = AngrybirdTaskRunner
else:
  RUNNER_CLASS = ProductionTaskRunner


def default_exit_action():
  sys.exit(0)


class ThermosExecutorTimer(ExceptionalThread):
  EXECUTOR_TIMEOUT = Amount(10, Time.SECONDS)

  def __init__(self, executor, driver):
    self._executor = executor
    self._driver = driver
    super(ThermosExecutorTimer, self).__init__()
    self.daemon = True

  def run(self):
    self._executor.launched.wait(self.EXECUTOR_TIMEOUT.as_(Time.SECONDS))
    if not self._executor.launched.is_set():
      self._executor.log('Executor timing out on lack of launchTask.')
      self._driver.stop()


class ThermosExecutor(ThermosExecutorBase):
  STOP_WAIT = Amount(5, Time.SECONDS)
  RESOURCE_CHECKPOINT = 'resource_usage.recordio'

  def __init__(self, runner_class=RUNNER_CLASS, manager_class=StatusManager):
    ThermosExecutorBase.__init__(self)
    self._runner = None
    self._task_id = None
    self._manager = None
    self._runner_class = runner_class
    self._manager_class = manager_class
    self._kill_manager = KillManager()
    # To catch killTasks sent while the runner initialization is occurring
    self._abort_runner = threading.Event()
    self.launched = threading.Event()

  @staticmethod
  def deserialize_assigned_task(task):
    """
      Deserialize task from a launchTask task protocol buffer.
      Returns AssignedTask
    """
    try:
      assigned_task = thrift_deserialize(AssignedTask(), task.data)
    except Exception as e:
      raise ValueError('Could not deserialize task! %s' % e)
    return assigned_task

  @staticmethod
  def deserialize_thermos_task(assigned_task):
    """
      Deserialize MesosTaskInstance from a AssignedTask thrift.
      Returns twitter.mesos.config.schema.MesosTaskInstance and the map of
      ports assigned by the scheduler.
    """
    thermos_task = assigned_task.task.thermosConfig
    if not thermos_task:
      raise ValueError('Task did not have a thermosConfig!')
    try:
      json_blob = json.loads(thermos_task)
    except Exception as e:
      raise ValueError('Could not deserialize thermosConfig JSON! %s' % e)
    return MesosTaskInstance(json_blob)

  @property
  def runner(self):
    return self._runner

  def _start_runner(self, driver, assigned_task, mesos_task, portmap):
    """
      Commence running a task.
        - Start the RunnerWrapper to fork TaskRunner (to control actual processes)
        - Set up necessary HealthCheckers
        - Set up ResourceCheckpointer
        - Set up StatusManager, and attach HealthCheckers
    """
    try:
      self._runner.initialize()
    except self._runner.TaskError as e:
      msg = 'Initialization of task runner failed: %s' % e
      log.fatal(msg)
      self.send_update(driver, self._task_id, 'FAILED', msg)
      defer(driver.stop, delay=self.STOP_WAIT)
      return

    if self._abort_runner.is_set():
      msg = 'Task killed during initialization'
      log.fatal(msg)
      self.send_update(driver, self._task_id, 'KILLED', msg)
      defer(driver.stop, delay=self.STOP_WAIT)
      return

    try:
      self._runner.start()
    except self._runner.TaskError as e:
      msg = 'Task initialization failed: %s' % e
      log.fatal(msg)
      self.send_update(driver, self._task_id, 'FAILED', msg)
      defer(driver.stop, delay=self.STOP_WAIT)
      return

    def wait_for_task():
      while not self._runner.is_started():
        log.debug('   - sleeping...')
        time.sleep(Amount(250, Time.MILLISECONDS).as_(Time.SECONDS))

    try:
      log.debug('Waiting for task to start.')
      deadline(wait_for_task, timeout=Amount(1, Time.MINUTES))
    except Timeout:
      msg = 'Timed out waiting for task to start!'
      log.fatal(msg)
      self._runner.lose()
      self.send_update(driver, self._task_id, 'LOST', msg)
      defer(driver.stop, delay=self.STOP_WAIT)
      return

    log.debug('Task started.')
    self.send_update(driver, self._task_id, 'RUNNING')

    health_checkers = []

    http_signaler = None
    if portmap.get('health'):
      http_signaler = HttpSignaler(portmap.get('health'))
      health_checkers.append(HealthCheckerThread(http_signaler.health,
          interval_secs=mesos_task.health_check_interval_secs().get()))

    task_path = TaskPath(root=self._runner._checkpoint_root, task_id=self._task_id)
    resource_manager = ResourceManager(
        mesos_task.task().resources(),
        TaskMonitor(task_path, self._task_id),
        self._runner.sandbox.root
    )
    health_checkers.append(resource_manager)

    ResourceCheckpointer(lambda: resource_manager.sample,
        os.path.join(self._runner.artifact_dir, self.RESOURCE_CHECKPOINT),
        recordio=True).start()

    if mesos_task.has_announce():
      discovery_manager = DiscoveryManager(
          mesos_task.role().get(),
          mesos_task.environment().get() if mesos_task.has_environment() else 'devel',
          assigned_task.task.jobName,
          socket.gethostname(),
          mesos_task.announce().primary_port().get(),
          portmap,
          assigned_task.task.shardId)
      health_checkers.append(discovery_manager)

    health_checkers.append(self._kill_manager)

    self._manager = self._manager_class(
        self._runner,
        driver,
        self._task_id,
        signaler=http_signaler,
        health_checkers=health_checkers)
    self._manager.start()

  """ Mesos Executor API methods follow """

  def resolve_ports(self, mesos_task, portmap):
    """Given a mesos task and the portmap of resolved ports from the scheduler,
       create a fully resolved map of port name => port number for the thermos
       runner and discovery manager."""
    task_portmap = mesos_task.announce().portmap().get() if mesos_task.has_announce() else {}
    task_portmap.update(portmap)
    task_portmap = PortResolver.resolve(task_portmap)

    for name, port in task_portmap.items():
      if not isinstance(port, int):
        log.warning('Task has unmapped port: %s => %s' % (name, port))

    return dict((name, port) for (name, port) in task_portmap.items() if isinstance(port, int))

  def launchTask(self, driver, task):
    """
      Invoked when a task has been launched on this executor (initiated via Scheduler::launchTasks).
      Note that this task can be realized with a thread, a process, or some simple computation,
      however, no other callbacks will be invoked on this executor until this callback has returned.
    """
    self.launched.set()
    self.log('launchTask got task: %s:%s' % (task.name, task.task_id.value))

    if self._runner:
      # TODO(wickman) Send LOST immediately for both tasks?
      log.error('Error!  Already running a task! %s' % self._runner)
      self.send_update(driver, self._task_id, 'LOST',
          "Task already running on this executor: %s" % self._task_id)
      return

    self._slave_id = task.slave_id.value
    self._task_id = task.task_id.value

    try:
      assigned_task = ThermosExecutor.deserialize_assigned_task(task)
      mesos_task = ThermosExecutor.deserialize_thermos_task(assigned_task)
    except Exception as e:
      log.fatal('Could not deserialize AssignedTask: %s' % e)
      self.send_update(driver, self._task_id, 'FAILED', "Could not deserialize task: %s" % e)
      defer(driver.stop, delay=self.STOP_WAIT)
      return

    self.send_update(driver, self._task_id, 'STARTING', 'Initializing sandbox.')

    # Fully resolve the portmap
    portmap = self.resolve_ports(mesos_task, assigned_task.assignedPorts)

    # start the process on a separate thread and give the message processing thread back
    # to the driver
    try:
      self._runner = self._runner_class(self._task_id, mesos_task, mesos_task.role().get(), portmap)
    except self._runner_class.TaskError as e:
      self.send_update(driver, self._task_id, 'FAILED', str(e))
      defer(driver.stop, delay=self.STOP_WAIT)
      return

    defer(lambda: self._start_runner(driver, assigned_task, mesos_task, portmap))

  def _signal_kill_manager(self, driver, task_id, reason):
    if self._runner is None:
      log.error('Was asked to kill task but no task running!')
      return
    if task_id.value != self._task_id:
      log.error('Asked to kill a task other than what we are running!')
      return
    if not self._runner.is_initialized():
      log.error('Asked to kill task with incomplete sandbox - aborting runner start')
      self._abort_runner.set()
      return
    self.log('Activating kill manager.')
    self._kill_manager.kill(reason)

  def killTask(self, driver, task_id):
    """
     Invoked when a task running within this executor has been killed (via
     SchedulerDriver::killTask). Note that no status update will be sent on behalf of the executor,
     the executor is responsible for creating a new TaskStatus (i.e., with TASK_KILLED) and invoking
     ExecutorDriver::sendStatusUpdate.
    """
    self.log('killTask got task_id: %s' % task_id)
    self._signal_kill_manager(driver, task_id, "Instructed to kill task.")

  def shutdown(self, driver):
    """
     Invoked when the executor should terminate all of its currently running tasks. Note that after
     Mesos has determined that an executor has terminated any tasks that the executor did not send
     terminal status updates for (e.g., TASK_KILLED, TASK_FINISHED, TASK_FAILED, etc) a TASK_LOST
     status update will be created.

    """
    self.log('shutdown called')
    if self._task_id:
      self._signal_kill_manager(driver, mesos_pb.TaskID(value=self._task_id),
          "Told to shut down executor.")
    self.log('shutdown returned')
