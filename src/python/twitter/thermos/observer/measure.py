import time
import threading

from twitter.common import log
from twitter.thermos.monitoring.pstree import ProcessSetFactory
from twitter.thermos.monitoring.sample_vector import SampleVector

from case_class import CaseClass

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

MeasuredTuple = CaseClass('task_id', 'process_name', 'process_run', 'process_pid')

class TaskMeasurer(threading.Thread):
  """
    Class responsible for polling CPU/RAM/DISK usage from live processes.
    Sublcassed from thread, runs in a background thread.  Control with start()/join().
  """

  class InternalError(Exception): pass
  SAMPLE_INTERVAL = 1.0 # seconds

  def __init__(self, muxer, interval = SAMPLE_INTERVAL):
    self._muxer       = muxer
    self._ps          = ProcessSetFactory.get()
    self._alive       = True
    self._interval    = interval
    self._processes   = {}    # MeasuredTuple -> SampleVector
    threading.Thread.__init__(self)

  def join(self):
    self._alive = False
    threading.Thread.join(self)

  @staticmethod
  def sleep_until(timestamp):
    time_now = time.time()
    total_sleep = timestamp - time_now
    log.debug('sleeping: %s' % total_sleep)
    if total_sleep > 0: time.sleep(total_sleep)

  def run(self):
    start = time.time()
    time_now = start

    while self._alive:
      last_interval = time_now
      TaskMeasurer.sleep_until(last_interval + self._interval)
      time_now = time.time()

      self._ps.refresh()

      for (uid, process, run) in self._muxer.get_active_processes():
        tup = MeasuredTuple(task_id = uid, process_name = process.process,
                            process_run = run, process_pid = process.pid)
        if tup not in self._processes:
          self._processes[tup] = SampleVector(process.process)
        children_pids = self._ps.get_children(process.pid)
        pcpu = 0.0
        for ph in children_pids:
          if ph._exists: pcpu += ph.pcpu
        self._processes[tup].add(time_now, pcpu / 100.0)

  def current_cpu_by_uid(self, task_id):
    processes = filter(
      lambda process: process.where(task_id = task_id) and (
        self._processes[process].num_samples() > 0),
      self._processes.keys())

    # TODO(wickman)  Figure out better abstraction here.
    last_samples = map(lambda process: self._processes[process].last_sample(), processes)
    if len(last_samples) == 0: return 0
    max_sample_time = max([s[0] for s in last_samples])
    pcpu = sum([s[1] for s in last_samples if s[0] == max_sample_time])
    if (time.time() - max_sample_time < 10): # arbitrary: 10seconds old
      return pcpu
    else:
      return 0

  # TODO(wickman)  Implement the proper O(n) solution.
  def current_cpu_by_process(self, task_id, process_name):
    processes = filter(
      lambda process: process.where(task_id = task_id, process_name = process_name) and (
          self._processes[process].num_samples() > 0),
        self._processes.keys())
    if len(processes) == 0: return 0

    if len(processes) > 1:
      raise TaskMeasurer.InternalError(
        "Unexpectedly large number of samples for task %s process %s" % (
          task_id, process_name))

    # TODO(wickman)  Memoize time.time() and get rid of all the hardcoded constants.
    last_sample = self._processes[processes[0]].last_sample()
    if (time.time() - last_sample[0] < 10): # arbitrary time here
      return last_sample[1]
    else:
      return 0

  def samples_by_uid(self, task_id):
    processes = filter(
      lambda process: process.where(task_id = task_id),
      self._processes.keys())
    return [self._processes[process] for process in processes]
