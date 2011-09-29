import os
import copy
import errno
import threading

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader

from gen.twitter.tcl.ttypes import ThermosJob
from gen.twitter.thermos.ttypes import *

from twitter.thermos.base import TaskPath
from twitter.thermos.base import TaskCkptDispatcher

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

# TODO(wickman) More descriptive class name, e.g. ProcessCheckpointMultiplexer
# TODO(wickman) Properly pydoc this all.
class TaskMuxer(object):
  """
    Class responsible for multiplexing incoming process checkpoint streams into a coherent
    task update stream.
  """

  class UnknownTask(Exception): pass

  def __init__(self, pathspec):
    self._pathspec      = pathspec
    self._uids          = set()
    self._ckpt_head     = {}
    self._runnerstate   = {}
    self._dispatcher    = {}
    self._lock          = threading.Lock()

  def lock(self):
    self._lock.acquire()

  def unlock(self):
    self._lock.release()

  # add a monitored uid
  def add(self, uid):
    self._uids.add(uid)
    self._ckpt_head[uid] = 0
    self._dispatcher[uid] = TaskCkptDispatcher()
    self._init_ckpt(uid)

  def pop(self, uid):
    self._uids.remove(uid)
    self._ckpt_head.pop(uid, None)
    self._runnerstate.pop(uid, None)
    self._dispatcher.pop(uid, None)

  def _apply_states(self, uid):
    """
      Given uid, os.stat() its corresponding checkpoint stream and determine if
      there are new ckpt records.  Attempt to read those records and update the
      high watermark for that stream.
    """
    if uid not in self._uids:
      raise TaskMuxer.UnknownTask('The task id %s is not being monitored!' % uid)

    uid_ckpt = self._pathspec.given(task_id = uid).getpath('runner_checkpoint')
    ckpt_offset = None
    try:
      ckpt_offset = os.stat(uid_ckpt).st_size
    except OSError, e:
      if e.errno == errno.ENOENT:
        log.error('Error in TaskMuxer: Could not read from discovered task %s' % uid_ckpt)
        return False
      else:
        raise

    updated = False
    if self._ckpt_head[uid] < ckpt_offset:
      # TODO(wickman)  Some of this logic should be factored out.  Right now
      # the select interface on Mac is broken, so we have to open/close on every
      # read, whereas on Linux you can keep persistent filehandles and reset eof.
      #
      # TODO(wickman)  Implement this using inotify on Linux platforms.
      with file(uid_ckpt, 'r') as fp:
        fp.seek(self._ckpt_head[uid])
        rr = ThriftRecordReader(fp, TaskRunnerCkpt)
        while True:
          runner_update = rr.try_read()
          if runner_update:
            self._dispatcher[uid].update_runner_state(self._runnerstate[uid], runner_update)
          else:
            break
        new_ckpt_head = fp.tell()
        updated = self._ckpt_head[uid] != new_ckpt_head
        if updated:
          self._ckpt_head[uid] = new_ckpt_head
    return updated

  def _init_ckpt(self, uid):
    self._runnerstate[uid] = TaskRunnerState(processes = {})
    self._apply_states(uid)

  # grab an intermediate runnerstate
  def get_state(self, uid):
    if uid in self._runnerstate:
      return copy.deepcopy(self._runnerstate[uid])
    return None

  # TODO(wickman)  Yielding with the lock is unsafe, fix this.
  def get_active_processes(self):
    self.lock()
    for uid in self._runnerstate:
      applied_states = self._apply_states(uid)
      log.debug('applied_states(%s) = %s' % (uid, applied_states))
      state = self._runnerstate[uid]
      for process in state.processes:
        if len(state.processes[process].runs) == 0:
          continue
        last_run = state.processes[process].runs[-1]
        if last_run.run_state == ProcessRunState.RUNNING:
          tup = (uid, last_run, len(state.processes[process].runs)-1)
          log.debug('yielding %s' % repr(tup))
          yield tup
    # TODO(wickman)  In the meantime look at all call-sites to verify sanity.
    self.unlock()
