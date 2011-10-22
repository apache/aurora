import time
import threading

from twitter.common import log
from twitter.common.process import ProcessProviderFactory
from twitter.thermos.observer.sample_vector import SampleVector

from case_class import CaseClass

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

# TODO(wickman)  After further consideration, this is probably the wrong tool for
# the job.  Consider replacing it.
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
    self._ps          = ProcessProviderFactory.get()
    self._alive       = True
    self._interval    = interval
    self._processes   = {}    # MeasuredTuple -> SampleVector
    threading.Thread.__init__(self)
    self.daemon = True

  def join(self):
    self._alive = False
    threading.Thread.join(self)

  @staticmethod
  def sleep_until(timestamp):
    time_now = time.time()
    total_sleep = timestamp - time_now
    if total_sleep > 0:
      time.sleep(total_sleep)

  def _filter_inactive_processes(self, active_processes):
    active_processes = set((tup[0], tup[1].process, tup[2]) for tup in active_processes)
    erase = []
    for measured_tuple in self._processes.keys():
      tup = (measured_tuple.get('task_id'), measured_tuple.get('process_name'),
             measured_tuple.get('process_run'))
      if tup not in active_processes:
        log.debug('TaskMeasurer unmonitoring %s' % repr(tup))
        erase.append(measured_tuple)
    for measured_tuple in erase:
      self._processes.pop(measured_tuple)

  def run(self):
    start = time.time()
    time_now = start

    while self._alive:
      last_interval = time_now
      TaskMeasurer.sleep_until(last_interval + self._interval)
      time_now = time.time()

      self._ps.collect_all()
      active_processes = [tup for tup in self._muxer.get_active_processes()]
      self._filter_inactive_processes(active_processes)

      for (uid, process, run) in active_processes:
        tup = MeasuredTuple(task_id = uid, process_name = process.process,
                            process_run = run, process_pid = process.pid)
        if tup not in self._processes:
          log.debug('TaskMeasurer monitoring %s' % tup)
          self._processes[tup] = SampleVector(process.process)
        if process.pid in self._ps.pids():
          children_pids = self._ps.children_of(process.pid)
          pcpu = 0.0
          for pid in children_pids:
            ph = self._ps.get_handle(pid)
            if ph.exists():
              pcpu += ph.cpu_time()
          self._processes[tup].add(time_now, pcpu)

  def current_cpu_by_clause(self, **clause):
    processes = filter(
      lambda process: process.where(**clause) and (
        self._processes[process].num_samples() > 1),
      self._processes.keys())

    def cpu_percentage(process):
      ((t0,s0), (t1,s1)) = self._processes[process].last_sample(2)
      return (t1, float((s1-s0) / (t1-t0)))
    last_samples = map(cpu_percentage, processes)
    if len(last_samples) == 0: return 0
    max_sample_time = max([s[0] for s in last_samples])
    pcpu = sum([s[1] for s in last_samples if s[0] == max_sample_time])
    # TODO(wickman)  Why is this 10 second delay there?
    if (time.time() - max_sample_time < 10):
      return pcpu
    else:
      return 0

  def current_cpu_by_uid(self, task_id):
    return self.current_cpu_by_clause(task_id = task_id)

  # TODO(wickman)  Implement the proper O(n) solution.
  def current_cpu_by_process(self, task_id, process_name):
    return self.current_cpu_by_clause(task_id = task_id, process_name = process_name)

  def samples_by_uid(self, task_id):
    processes = filter(
      lambda process: process.where(task_id = task_id),
      self._processes.keys())
    return [self._processes[process] for process in processes]
