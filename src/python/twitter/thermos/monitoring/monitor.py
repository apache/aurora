import os
import copy
import errno
import threading

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader
from twitter.thermos.base.ckpt import CheckpointDispatcher
from gen.twitter.thermos.ttypes import (
  ProcessState,
  RunnerCkpt,
  RunnerState,
  TaskState)

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class TaskMonitor(object):
  """
    Class responsible for monitoring an individual Thermos Task via its runner checkpoint.
  """

  def __init__(self, pathspec, task_id):
    self._task_id = task_id
    self._dispatcher = CheckpointDispatcher()
    self._runnerstate = RunnerState(processes={})
    self._runner_ckpt = pathspec.given(task_id=task_id).getpath('runner_checkpoint')
    self._ckpt_head = 0
    self._apply_states()
    self._lock = threading.Lock()

  def _apply_states(self):
    """
      Given task_id, os.stat() its corresponding checkpoint stream and
      determine if there are new ckpt records.  Attempt to read those
      records and update the high watermark for that stream.  Returns
      true if new states were applied.
    """
    ckpt_offset = None
    try:
      ckpt_offset = os.stat(self._runner_ckpt).st_size

      updated = False
      if self._ckpt_head < ckpt_offset:
        with open(self._runner_ckpt, 'r') as fp:
          fp.seek(self._ckpt_head)
          rr = ThriftRecordReader(fp, RunnerCkpt)
          while True:
            runner_update = rr.try_read()
            if not runner_update:
              break
            try:
              self._dispatcher.dispatch(self._runnerstate, runner_update)
            except CheckpointDispatcher.InvalidSequenceNumber as e:
              log.error('Checkpoint stream is corrupt: %s' % e)
              break
          new_ckpt_head = fp.tell()
          updated = self._ckpt_head != new_ckpt_head
          self._ckpt_head = new_ckpt_head
      return updated
    except OSError as e:
      if e.errno == errno.ENOENT:
        # The log doesn't yet exist, will retry later.
        log.warning('Could not read from discovered task %s.' % self._task_id)
        return False
      else:
        raise

  def refresh(self):
    """
      Check to see if there are new updates and apply them.  Return true if
      updates were applied, false otherwise.
    """
    with self._lock:
      return self._apply_states()

  def get_state(self):
    """
      Get the latest state of the task_id.
    """
    with self._lock:
      self._apply_states()
      return copy.deepcopy(self._runnerstate)

  def task_state(self):
    state = self.get_state()
    return state.statuses[-1].state if state.statuses else TaskState.ACTIVE

  def get_active_processes(self):
    """
      Get active processes.  Returned is a list of tuples of the form:
        (ProcessStatus object of running object, its run number)
    """
    active_processes = []
    with self._lock:
      self._apply_states()
      state = self._runnerstate
      for process, runs in state.processes.items():
        if len(runs) == 0:
          continue
        last_run = runs[-1]
        if last_run.state == ProcessState.RUNNING:
          active_processes.append((last_run, len(runs) - 1))
    return active_processes
