import os
import time
import select as fdselect

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader

from thermos_thrift.ttypes  import *
from twitter.thermos.runner.task import WorkflowTask

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class WorkflowTaskMuxer_SomethingBadHappened(Exception): pass

class WorkflowTaskMuxer:
  LOST_TIMEOUT = 60.0  # if the pidfile hasn't shown up after this long, consider the task LOST

  def __init__(self):
    self._unbound_tasks = []
    self._pid_to_task   = {}
    self._pid_to_fp     = {}
    self._pid_to_offset = {}
    self._lost_pids     = set([])

  def register(self, workflow_task):
    log.debug('registering %s' % workflow_task)

    pid = workflow_task.pid()
    if not pid:
      self._unbound_tasks.append(workflow_task)
    elif pid in self._pid_to_task:
      raise WorkflowTaskMuxer_SomethingBadHappened()
    else:
      self._pid_to_task[pid] = workflow_task

  def _bind_tasks(self):
    new_unbound_tasks = []
    for task in self._unbound_tasks:
      if task.pid() is not None:
        self._pid_to_task[task.pid()] = task
      else:
        new_unbound_tasks.append(task)
    self._unbound_tasks = new_unbound_tasks

  def _try_register_pid_fp(self, pid):
    pidfile = self._pid_to_task[pid].ckpt_file()
    if os.path.exists(pidfile):
      try:
        self._pid_to_fp[pid] = file(pidfile, "r")
        self._pid_to_offset[pid] = 0
        return True
      except OSError, e:
        log.error("Unable to open pidfile %s (%s)" % (pidfile, e))
    return False

  def _try_open_pid_files(self):
    registered_pids = set(self._pid_to_task.keys())
    opened_pids     = set(self._pid_to_fp.keys())
    # wait for pid files to come into existence.
    # if they haven't come into existence fast enough, tasks are LOST
    time_now = time.time()
    for pid in (registered_pids - opened_pids):
      if not self._try_register_pid_fp(pid):
        if (time_now - self._pid_to_task[pid].fork_time()) > WorkflowTaskMuxer.LOST_TIMEOUT:
          if pid not in self._lost_pids:
            self._lost_pids.add(pid)
            # TODO(wickman) XXX do something here
            # self._updates.append(self._pid_to_task[pid].lost_update())

  # This is needlessly complicated because of http://bugs.python.org/issue1706039
  # Should work w/o it on Linux, but necessary on OS X.
  def _try_reopen_pid_files(self, broken):
    for (pid, fp) in self._pid_to_fp.iteritems():
      if fp in broken:
        fp.close()
        log.debug("undoing EOF for %s" % self._pid_to_task[pid].ckpt_file())
        # XXX try/except!!!
        self._pid_to_fp[pid] = file(self._pid_to_task[pid].ckpt_file(), "r")
        self._pid_to_fp[pid].seek(self._pid_to_offset[pid])

  def _find_pid_by_fp(self, fp):
    for (pid, filep) in self._pid_to_fp.iteritems():
      if fp == filep: return pid
    return None

  def _select_local_updates(self):
    updates = []
    for pid, task in self._pid_to_task.iteritems():
      update = task.initial_update()
      if update: updates.append(update)
    log.debug('fdselect._local = %s' % updates)
    return updates

  # returns a list of wts objects
  def select(self, timeout = 1.0):
    self._bind_tasks()
    self._try_open_pid_files()

    # get fds with data ready
    pid_files = self._pid_to_fp.values()
    log.debug('fdselect.select(%s)' % pid_files)
    ready, _, broken = fdselect.select(pid_files, [], pid_files, timeout)

    ready_pids = map(lambda fp: self._find_pid_by_fp(fp), ready)

    log.debug('ready: %s' % ' '.join(map(lambda f: f.name, ready)))
    if len(broken) > 0:
      log.debug('ERROR! broken fds = %s' % ' '.join(map(lambda f: f.name, broken)))
      self._try_reopen_pid_files(broken)

    local_updates = self._select_local_updates()
    updates = []
    for pid in ready_pids:
      ckpt = self._pid_to_fp[pid]
      rr = ThriftRecordReader(ckpt, WorkflowRunnerCkpt)
      while True:
        wts = rr.try_read()
        if wts:
          updates.append(wts)
        else:
          self._pid_to_offset[pid] = ckpt.tell()
          break
    log.debug('watcher returning %s [%s local, %s out-of-process] updates ' % (
      len(local_updates) + len(updates), len(local_updates), len(updates)))
    updates = list(local_updates) + updates
    return updates

  def lost(self):
    self._try_open_pid_files()
    return map(lambda p: self._pid_to_task[p].name(), self._lost_pids)

  def _find_pid_by_task(self, task):
    pid = None
    for find_pid in self._pid_to_task:
      if self._pid_to_task[find_pid].name() == task:
        pid = find_pid
        break
    return pid

  def unregister(self, task_name):
    pid = self._find_pid_by_task(task_name)
    if pid is not None:
      for d in [self._pid_to_fp, self._pid_to_task, self._pid_to_offset]:
        d.pop(pid, None)
      if pid in self._lost_pids:
        self._lost_pids.remove(pid)
    else:
      found_task = None
      for task in self._unbound_tasks:
        if task.name() == task_name:
          found_task = task
          break
      if found_task is None:
        raise WorkflowTaskMuxer_SomethingBadHappened("No trace of task: %s" % task_name)
      else:
        self._unbound_tasks.remove(found_task)
    return pid
