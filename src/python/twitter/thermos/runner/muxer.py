import os
import errno

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader
from gen.twitter.thermos.ttypes import RunnerCkpt

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class ProcessMuxer(object):
  class ProcessExists(Exception): pass
  class ProcessNotFound(Exception): pass
  class CorruptCheckpoint(Exception): pass

  def __init__(self, pathspec):
    self._processes = {} # process_name => fp
    self._watermarks = {} # process_name => sequence high watermark
    self._pathspec = pathspec

  def __del__(self):
    for fp in filter(None, self._processes.values()):
      fp.close()

  def register(self, process_name, watermark=0):
    log.debug('registering %s' % process_name)
    if process_name in self._processes:
      raise ProcessMuxer.ProcessExists("Process %s is already registered" % task_process.name())
    self._processes[process_name] = None
    self._watermarks[process_name] = watermark

  def _bind_processes(self):
    for process_name, fp in self._processes.items():
      if fp is None:
        process_ckpt = self._pathspec.given(process=process_name).getpath('process_checkpoint')
        log.debug('ProcessMuxer binding %s => %s' % (process_name, process_ckpt))
        try:
          self._processes[process_name] = open(process_ckpt, 'r')
        except IOError as e:
          if e.errno == errno.ENOENT:
            log.debug('  => bind failed, checkpoint not available yet.')
            continue
          else:
            log.error("Unexpected inability to open %s! %s" % (process_ckpt, e))
        except Exception as e:
          log.error("Unexpected inability to open %s! %s" % (process_ckpt, e))
        self._fast_forward_stream(process_name)

  def _fast_forward_stream(self, process_name):
    assert self._processes.get(process_name) is not None
    rr = ThriftRecordReader(self._processes[process_name], RunnerCkpt)
    current_watermark = -1
    records = 0
    while current_watermark < self._watermarks[process_name]:
      record = rr.read()
      records += 1
      if record is None:
        raise ProcessMuxer.CorruptCheckpoint('Could not reach high watermark for %s@sequence=%s' %
          process_name, self._watermarks[process_name])
      else:
        current_watermark = record.process_status.seq
    if records:
      log.debug('Fast forwarded %s %s record(s).' % (self._processes[process_name].name, records))

  def unregister(self, process_name):
    log.debug('unregistering %s' % process_name)
    if process_name not in self._processes:
      raise ProcessMuxer.ProcessNotFound("No trace of process: %s" % process_name)
    else:
      self._watermarks.pop(process_name)
      fp = self._processes.pop(process_name)
      if fp is not None:
        fp.close()

  def has_data(self, process):
    """
      Return true if we think that there are updates available from the supplied process.
    """
    self._bind_processes()
    # TODO(wickman) Should this raise ProcessNotFound?
    if process not in self._processes:
      return False
    fp = self._processes[process]
    rr = ThriftRecordReader(fp, RunnerCkpt)
    old_pos = fp.tell()
    try:
      expected_new_pos = os.fstat(fp.fileno()).st_size
    except OSError as e:
      log.debug('ProcessMuxer could not fstat for process %s' % process)
      return False
    update = rr.try_read()
    if update:
      fp.seek(old_pos)
      return True
    return False

  def select(self):
    """
      Read and multiplex checkpoint records from all the forked off process managers.

      Checkpoint records can come from one of two places:
        in-process: checkpoint records synthesized for FORKED and LOST events
        out-of-process: checkpoint records from from file descriptors of forked managers

      Returns a list of RunnerCkpt objects that were successfully read, or an empty
      list if none were read.
    """
    self._bind_processes()
    updates = []
    for handle in filter(None, self._processes.values()):
      try:
        fstat = os.fstat(handle.fileno())
      except OSError as e:
        log.error('Unable to fstat %s!' % handle.name)
        continue
      if handle.tell() > fstat.st_size:
        log.error('Truncated checkpoint record detected on %s!' % handle.name)
      elif handle.tell() < fstat.st_size:
        rr = ThriftRecordReader(handle, RunnerCkpt)
        while True:
          process_update = rr.try_read()
          if process_update:
            updates.append(process_update)
          else:
            break
    if len(updates) > 0:
      log.debug('select() returning %s updates:' % len(updates))
      for update in updates:
        log.debug('  = %s' % update)
    return updates
