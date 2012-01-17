import os
import time
import select as fdselect
import errno

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader
from twitter.common.quantity import Amount, Time

from gen.twitter.thermos.ttypes import *
from twitter.thermos.runner.process import Process

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class ProcessMuxer(object):
  class ProcessExists(Exception): pass
  class ProcessNotFound(Exception): pass

  def __init__(self):
    self._processes = {} # process_name => Process()
    self._fps = {}       # process_name => fp

  def __del__(self):
    for fp in self._fps.values():
      fp.close()

  def register(self, task_process):
    log.debug('registering %s' % task_process)
    if task_process.name() in self._processes:
      raise ProcessMuxer.ProcessExists("Process %s is already registered" % task_process.name())
    self._processes[task_process.name()] = task_process

  def unregister(self, process_name):
    if process_name not in self._processes:
      raise ProcessMuxer.ProcessNotFound("No trace of process: %s" % process_name)
    self._processes.pop(process_name)
    if process_name in self._fps:
      self._fps[process_name].close()
      self._fps.pop(process_name)

  def _bind_processes(self):
    for process in self._processes.values():
      if process.name() not in self._fps and process.pid() is not None:
        try:
          self._fps[process.name()] = open(process.ckpt_file(), 'r')
        except IOError as e:
          if e.errno == errno.ENOENT:
            log.debug('ProcessMuxer._bind_processes(%s) failed, checkpoint not available yet.' %
              process.name())
            continue
          else:
            log.error("Unexpected inability to open %s! %s" % (process.ckpt_file(), e))
        except Exception as e:
          log.error("Unexpected inability to open %s! %s" % (process.ckpt_file(), e))

  def _select_local_updates(self, from_processes=None):
    updates = []
    if from_processes is None:
      from_processes = self._processes.keys()
    for process_name, process in self._processes.items():
      if process_name not in from_processes: continue
      update = process.initial_update()
      if update:
        updates.append(update)
    return updates

  def has_data(self, process):
    """
      Return true if we think that there are updates available from the supplied process.
    """
    if process in self._processes and self._processes[process].has_initial_update():
      return True
    if process not in self._fps:
      return False
    fp = self._fps[process]
    rr = ThriftRecordReader(fp, TaskRunnerCkpt)
    old_pos = fp.tell()
    try:
      expected_new_pos = os.fstat(fp.fileno()).st_size
    except OSError as e:
      log.debug('ProcessMuxer.has_data could not fstat for process %s' % process)
      return False
    update = rr.try_read()
    if update:
      fp.seek(old_pos)
      return True
    return False

  def select(self, from_processes=None):
    """
      Read and multiplex checkpoint records from all the forked off process managers.

      Checkpoint records can come from one of two places:
        in-process: checkpoint records synthesized for FORKED and LOST events
        out-of-process: checkpoint records from from file descriptors of forked managers

      Returns a list of TaskRunnerCkpt objects that were successfully read, or an empty
      list if none were read.
    """
    self._bind_processes()
    if from_processes is None:
      from_processes = self._fps.keys()

    updates = []
    for (process, fp) in self._fps.items():
      if process not in from_processes: continue
      try:
        fstat = os.fstat(fp.fileno())
      except OSError as e:
        log.error('Unable to fstat %s!' % fp.name)
        continue
      if fp.tell() > fstat.st_size:
        log.error('Truncated checkpoint record detected on %s!' % fp.name)
      elif fp.tell() < fstat.st_size:
        rr = ThriftRecordReader(fp, TaskRunnerCkpt)
        while True:
          process_update = rr.try_read()
          if process_update:
            updates.append(process_update)
          else:
            break
    # N.B. wickman: Restricting select_local_updates to from_processes is the wrong thing
    #  and created a subtle race condition bug.
    local_updates = list(self._select_local_updates())
    if len(local_updates) > 0 or len(updates) > 0:
      log.debug('select() returning %s local, %s out-of-process updates:' % (
        len(local_updates), len(updates)))
      for update in local_updates:
        log.debug('  local: %s' % update)
      for update in updates:
        log.debug('  o-o-p: %s' % update)
    return list(local_updates) + updates
