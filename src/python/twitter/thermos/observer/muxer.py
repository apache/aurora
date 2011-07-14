import os
import copy
import errno
import threading

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader

from tcl_thrift.ttypes import ThermosJob
from thermos_thrift.ttypes import *

from twitter.thermos.base import WorkflowPath
from twitter.thermos.base import WorkflowCkptDispatcher

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class WorkflowMuxer:
  """
    Class responsible for multiplexing incoming task checkpoint streams into a coherent
    workflow update stream.
  """

  def __init__(self, pathspec):
    self._pathspec      = pathspec
    self._uids          = set([])
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
    self._dispatcher[uid] = WorkflowCkptDispatcher()
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
    uid_ckpt = self._pathspec.given(job_uid = uid).getpath('runner_checkpoint')
    ckpt_offset = None
    try:
      ckpt_offset = os.stat(uid_ckpt).st_size
    except OSError, e:
      if e.errno == errno.EEXIST:
        ckpt_offset = 0
      else: raise
    updated = False
    if self._ckpt_head[uid] < ckpt_offset:
      # TODO(wickman):  Some of this logic should be factored out.  Right now
      # the select interface on Mac is broken, so we have to open/close on every
      # read, whereas on Linux you can keep persistent filehandles and reset eof.
      with file(uid_ckpt, 'r') as fp:
        fp.seek(self._ckpt_head[uid])
        rr = ThriftRecordReader(fp, WorkflowRunnerCkpt)
        while True:
          runner_update = rr.try_read()
          if runner_update:
            self._dispatcher[uid].update_runner_state(
              self._runnerstate[uid], runner_update)
          else:
            break
        updated = self._ckpt_head[uid] != fp.tell()
        if updated: self._ckpt_head[uid] = fp.tell()
    return updated

  def _init_ckpt(self, uid):
    self._runnerstate[uid] = WorkflowRunnerState(tasks = {})
    self._apply_states(uid)

  # grab an intermediate runnerstate
  def get_state(self, uid):
    if uid in self._runnerstate:
      return copy.deepcopy(self._runnerstate[uid])
    return None

  def get_active_tasks(self):
    self.lock()
    for uid in self._runnerstate:
      applied_states = self._apply_states(uid)
      log.debug('applied_states(%s) = %s' % (uid, applied_states))
      state = self._runnerstate[uid]
      for task in state.tasks:
        if len(state.tasks[task].runs) == 0: continue
        last_run = state.tasks[task].runs[-1]
        if last_run.run_state == WorkflowTaskRunState.RUNNING:
          tup = (uid, state.header.workflow_name, last_run, len(state.tasks[task].runs)-1)
          log.debug('yielding %s' % repr(tup))
          yield tup
    # TODO(wickman): look at all callsites to verify sanity
    self.unlock()
