import mesos
import os
import pwd
import time

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.process import ProcessProviderFactory
from twitter.common.quantity import Amount, Time, Data
from twitter.thermos.base.path import TaskPath
from twitter.thermos.runner.inspector import CheckpointInspector
from twitter.thermos.runner.runner import TaskRunner
from twitter.thermos.monitoring.detector import TaskDetector
from twitter.thermos.monitoring.garbage import (
  TaskGarbageCollector,
  DefaultCollector)
from twitter.mesos.executor.sandbox_manager import (
  DirectorySandbox,
  AppAppSandbox)
from twitter.mesos.executor.executor_base import ThermosExecutorBase
from gen.twitter.mesos.comm.ttypes import AdjustRetainedTasks
from gen.twitter.mesos.ttypes import ScheduleStatus

# thrifts
from thrift.TSerialization import deserialize as thrift_deserialize

app.add_option("--checkpoint_root", dest="checkpoint_root", metavar="PATH",
               default="/var/run/thermos",
               help="the checkpoint root from which we garbage collect")


class ThermosTaskGarbageCollector(TaskGarbageCollector):
  @classmethod
  def _log(cls, msg):
    log.warning('TaskGarbageCollector: %s' % msg)


class ThermosGCExecutor(ThermosExecutorBase):
  """
    Thermos GC Executor, responsible for:
      - garbage collecting old tasks to make sure they don't clutter up the system
      - state reconciliation with the scheduler (in case it thinks we're running
        something we're not or vice versa.)
  """
  MAX_PID_TIME_DRIFT = Amount(10, Time.SECONDS)
  MAX_CHECKPOINT_TIME_DRIFT = Amount(1, Time.HOURS)  # maximum runner disconnection time

  def __init__(self, max_age=Amount(14, Time.DAYS),
                     max_space=Amount(200, Data.GB),
                     max_tasks=1000,
                     verbose=True,
                     task_runner_factory=TaskRunner.get,
                     checkpoint_root=None):
    ThermosExecutorBase.__init__(self)
    self._slave_id = None
    self._gc_options = dict(
      max_age=max_age,
      max_space=max_space,
      max_tasks=max_tasks,
      verbose=verbose,
      logger=self.log
    )
    self._task_runner_factory = task_runner_factory
    if 'ANGRYBIRD_THERMOS' in os.environ:
      self._checkpoint_root = os.path.join(os.environ['ANGRYBIRD_THERMOS'], 'thermos/run')
    else:
      self._checkpoint_root = checkpoint_root or app.get_options().checkpoint_root

  def reconcile_states(self, driver, retained_tasks):
    self.log('Told to retain the following task ids:')
    for task_id, schedule_status in retained_tasks.items():
      self.log('  => %s as %s' % (task_id, ScheduleStatus._VALUES_TO_NAMES[schedule_status]))

    def terminate_task(task_id, kill=True):
      runner = self._task_runner_factory(task_id, self._checkpoint_root)
      if runner is None:
        self.log('Could not terminate task %s because we could not bind to its TaskRunner.'
             % task_id)
        return False
      self.log('Terminating %s...' % task_id)
      runner_terminate = runner.kill if kill else runner.lose
      try:
        runner_terminate(force=True)
        return True
      except Exception as e:
        self.log('Could not terminate: %s' % e)
        return False

    detector = TaskDetector(root=self._checkpoint_root)
    active_tasks = set(t_id for _, t_id in detector.get_task_ids(state='active'))
    finished_tasks = set(t_id for _, t_id in detector.get_task_ids(state='finished'))

    def get_state(task_id):
      pathspec = TaskPath(root=self._checkpoint_root, task_id=task_id)
      runner_ckpt = pathspec.getpath('runner_checkpoint')
      state = CheckpointDispatcher.from_file(runner_ckpt)
      return runner_ckpt, state

    terminated_tasks = set()
    for task_id, task_state in retained_tasks.items():
      if task_id not in active_tasks and task_id not in finished_tasks:
        self.send_update(driver, task_id, 'LOST',
          'GC executor could find no trace of %s.' % task_id)
      elif task_id in active_tasks and self.twitter_status_is_terminal(task_state):
        log._info('Scheduler thinks active task %s is in terminal state, killing.' % task_id)
        if terminate_task(task_id):
          self.send_update(driver, task_id, 'KILLED',
            'Scheduler thought %s was in terminal state so we killed it.' % task_id)
          terminated_tasks.add(task_id)
      elif task_id in finished_tasks and not self.twitter_status_is_terminal(task_state):
        _, state = get_state(task_id)
        if state is None or state.statuses is None:
          self.log('Failed to replay %s, state: %s' % (runner_ckpt, state))
          continue
        self.send_update(driver, task_id, state.statuses[-1].state,
          'Scheduler thinks finished task %s is active, sending terminal state.' % task_id)

    # Inspect checkpoints for trees that have been kill -9'ed but not properly cleaned up.
    inspector = CheckpointInspector(self._checkpoint_root)
    ps = ProcessProviderFactory.get()
    ps.collect_all()

    # TODO(wickman) This is almost a replica of code in runner/runner.py, factor out.
    def is_our_pid(pid, uid, timestamp):
      handle = ps.get_handle(pid)
      if handle.user() != pwd.getpwuid(uid)[0]: return False
      estimated_start_time = time.time() - handle.wall_time()
      return abs(timestamp - estimated_start_time) < self.MAX_PID_TIME_DRIFT.as_(Time.SECONDS)

    for task_id in active_tasks - terminated_tasks:
      log.info('Inspecting running task: %s' % task_id)
      inspection = inspector.inspect(task_id)
      latest_runner = inspection.runner_processes[-1]
      # Assume that it has not yet started?
      if not latest_runner:
        log.warning('  - Task has no registered runners.')
        continue
      runner_pid, runner_uid, runner_timestamp = latest_runner
      if runner_pid in ps.pids() and is_our_pid(runner_pid, runner_uid, runner_timestamp):
        log.info('  - Runner appears healthy.')
        continue
      # Runner is dead
      runner_ckpt = TaskPath(root=self._checkpoint_root, task_id=task_id).getpath(
          'runner_checkpoint')
      latest_update = os.path.getmtime(runner_ckpt)
      if time.time() - latest_update < self.MAX_CHECKPOINT_TIME_DRIFT.as_(Time.SECONDS):
        log.info('  - Runner is dead but under LOST threshold.')
        continue
      log.info('  - Runner is dead but beyond LOST threshold: %.1fs' % (
          time.time() - latest_update))
      if terminate_task(task_id, kill=False):
        self.send_update(driver, task_id, 'LOST',
          'Reporting task %s as LOST because its runner has been dead too long.' % task_id)

  def garbage_collect_task(self, task_id, task_garbage_collector):
    directory_sandbox = DirectorySandbox(task_id)
    if directory_sandbox.exists():
      self.log('Destroying DirectorySandbox for %s' % task_id)
      directory_sandbox.destroy()
    else:
      appapp_sandbox = AppAppSandbox(task_id)
      if appapp_sandbox.exists():
        self.log('Destroying AppAppSandbox for %s' % task_id)
        appapp_sandbox.destroy()
      else:
        self.log('ERROR: Could not identify the sandbox manager for %s!' % task_id)
    self.log('Erasing logs for %s' % task_id)
    task_garbage_collector.erase_logs(task_id)
    self.log('Erasing metadata for %s' % task_id)
    task_garbage_collector.erase_metadata(task_id)

  def garbage_collect(self, retained_task_ids):
    tgc = ThermosTaskGarbageCollector(root=self._checkpoint_root)
    gc_tasks = DefaultCollector(tgc, **self._gc_options).run()
    gc_task_ids = set(task.task_id for task in gc_tasks)
    for task_id in set(retained_task_ids).intersection(gc_task_ids):
      self.log('Skipping garbage collection for %s because explicitly directed by scheduler.' %
        task_id)
    for task_id in gc_task_ids - set(retained_task_ids):
      self.garbage_collect_task(task_id, tgc)

  def launchTask(self, driver, task):
    self._slave_id = task.slave_id.value
    self.log('launchTask called.')
    retain_tasks = AdjustRetainedTasks()
    thrift_deserialize(retain_tasks, task.data)
    self.reconcile_states(driver, retain_tasks.retainedTasks)
    self.garbage_collect(retain_tasks.retainedTasks.keys())
    driver.stop()

  def killTask(self, driver, task_id):
    self.log('killTask() got task_id: %s, ignoring.' % task_id)

  def shutdown(self, driver):
    self.log('shutdown() called, ignoring.')


LogOptions.set_log_dir('/var/log/mesos')
app.configure(module='twitter.common.app.modules.scribe_exception_handler',
    category='test_thermos_gc_executor_exceptions')
app.configure(debug=True)

def main():
  LogOptions.set_disk_log_level('DEBUG')
  thermos_gc_executor = ThermosGCExecutor()
  drv = mesos.MesosExecutorDriver(thermos_gc_executor)
  drv.run()
  log.info('MesosExecutorDriver.run() has finished.')

app.main()
