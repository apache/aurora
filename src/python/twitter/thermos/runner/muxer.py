import os
import time
import select as fdselect

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader

from gen.twitter.thermos.ttypes import *
from twitter.thermos.runner.process import Process

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class ProcessMuxer(object):
  class ProcessExists(Exception): pass
  class ProcessNotFound(Exception): pass

  LOST_TIMEOUT = 60.0  # if the pidfile hasn't shown up after this long, consider the process LOST

  def __init__(self):
    self._unbound_processes = []
    self._pid_to_process   = {}
    self._pid_to_fp     = {}
    self._pid_to_offset = {}
    self._lost_pids     = set([])

  def register(self, task_process):
    log.debug('registering %s' % task_process)

    pid = task_process.pid()
    if not pid:
      self._unbound_processes.append(task_process)
    elif pid in self._pid_to_process:
      raise ProcessMuxer.ProcessExists("Pid %s is already registered" % pid)
    else:
      self._pid_to_process[pid] = task_process

  def _bind_processes(self):
    new_unbound_processes = []
    for process in self._unbound_processes:
      if process.pid() is not None:
        self._pid_to_process[process.pid()] = process
      else:
        new_unbound_processes.append(process)
    self._unbound_processes = new_unbound_processes

  def _try_register_pid_fp(self, pid):
    pidfile = self._pid_to_process[pid].ckpt_file()
    if os.path.exists(pidfile):
      try:
        self._pid_to_fp[pid] = file(pidfile, "r")
        self._pid_to_offset[pid] = 0
        return True
      except OSError, e:
        log.error("Unable to open pidfile %s (%s)" % (pidfile, e))
    return False

  def _try_open_pid_files(self):
    registered_pids = set(self._pid_to_process.keys())
    opened_pids     = set(self._pid_to_fp.keys())
    # wait for pid files to come into existence.
    # if they haven't come into existence fast enough, processes are LOST
    time_now = time.time()
    for pid in (registered_pids - opened_pids):
      if not self._try_register_pid_fp(pid):
        if (time_now - self._pid_to_process[pid].fork_time()) > ProcessMuxer.LOST_TIMEOUT:
          if pid not in self._lost_pids:
            self._lost_pids.add(pid)
            # TODO(wickman) XXX - LOST state updates are not yet enqueued onto the
            # checkpoint stream.

  # This is needlessly complicated because of http://bugs.python.org/issue1706039
  # Should work w/o it on Linux, but necessary on OS X.
  def _try_reopen_pid_files(self, broken):
    for (pid, fp) in self._pid_to_fp.iteritems():
      if fp in broken:
        fp.close()
        log.debug("undoing EOF for %s" % self._pid_to_process[pid].ckpt_file())
        # TODO(wickman) XXX - This should be wrapped in a try/except
        self._pid_to_fp[pid] = file(self._pid_to_process[pid].ckpt_file(), "r")
        self._pid_to_fp[pid].seek(self._pid_to_offset[pid])

  def _find_pid_by_fp(self, fp):
    for (pid, filep) in self._pid_to_fp.iteritems():
      if fp == filep: return pid
    return None

  def _select_local_updates(self):
    updates = []
    for pid, process in self._pid_to_process.iteritems():
      update = process.initial_update()
      if update: updates.append(update)
    log.debug('fdselect._local = %s' % updates)
    return updates

  # TODO(wickman)  pydoc this.
  def select(self, timeout = 1.0):
    self._bind_processes()
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
      rr = ThriftRecordReader(ckpt, TaskRunnerCkpt)
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
    return map(lambda p: self._pid_to_process[p].name(), self._lost_pids)

  def _find_pid_by_process(self, process):
    pid = None
    for find_pid in self._pid_to_process:
      if self._pid_to_process[find_pid].name() == process:
        pid = find_pid
        break
    return pid

  def unregister(self, process_name):
    pid = self._find_pid_by_process(process_name)
    if pid is not None:
      for d in (self._pid_to_fp, self._pid_to_process, self._pid_to_offset):
        d.pop(pid, None)
      if pid in self._lost_pids:
        self._lost_pids.remove(pid)
    else:
      found_process = None
      for process in self._unbound_processes:
        if process.name() == process_name:
          found_process = process
          break
      if found_process is None:
        raise ProcessMuxer.ProcessNotFound("No trace of process: %s" % process_name)
      else:
        self._unbound_processes.remove(found_process)
    return pid
