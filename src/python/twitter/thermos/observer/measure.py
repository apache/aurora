import time
import threading

import twitter.common.log
log = twitter.common.log.get()

from twitter.thermos.monitoring.pstree import ProcessSetFactory
from twitter.thermos.monitoring.sample_vector import SampleVector

from case_class import CaseClass

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

MeasuredTuple = CaseClass('job_uid', 'workflow_name', 'task_name', 'task_run', 'task_pid')

class WorkflowMeasurer_InternalError(Exception): pass
class WorkflowMeasurer(threading.Thread):
  """
    Class responsible for polling CPU/RAM/DISK usage from live tasks.
    Sublcassed from thread, runs in a background thread.  Control with start()/join().
  """

  SAMPLE_INTERVAL = 1.0 # seconds

  def __init__(self, muxer, interval = SAMPLE_INTERVAL):
    self._muxer       = muxer
    self._ps          = ProcessSetFactory.get()
    self._alive       = True
    self._interval    = interval
    self._tasks       = {}    # MeasuredTuple -> SampleVector
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
      WorkflowMeasurer.sleep_until(last_interval + self._interval)
      time_now = time.time()

      self._ps.refresh()

      for (uid, workflow_name, task, run) in self._muxer.get_active_tasks():
        tup = MeasuredTuple(job_uid = uid, workflow_name = workflow_name,
                            task_name = task.task,
                            task_run  = run,
                            task_pid  = task.pid)
        if tup not in self._tasks:
          self._tasks[tup] = SampleVector(task.task)
        children_pids = self._ps.get_children(task.pid)
        pcpu = 0.0
        for ph in children_pids:
          if ph._exists: pcpu += ph.pcpu
        self._tasks[tup].add(time_now, pcpu / 100.0)

  def current_cpu_by_uid(self, job_uid):
    tasks = filter(lambda task: task.where(job_uid = job_uid) and self._tasks[task].num_samples() > 0,
                   self._tasks.keys())

    # ugh breaking abstraction barriers big time here
    last_samples = map(lambda task: self._tasks[task].last_sample(), tasks)
    if len(last_samples) == 0: return 0
    max_sample_time = max([s[0] for s in last_samples])
    pcpu = sum([s[1] for s in last_samples if s[0] == max_sample_time])
    if (time.time() - max_sample_time < 10): # arbitrary: 10seconds old
      return pcpu
    else:
      return 0

  # the way we're using this now is basically N^2 -- i.e slow as balls.  fix it.
  def current_cpu_by_task(self, job_uid, workflow_name, task_name):
    tasks = filter(
      lambda task: task.where(job_uid = job_uid,
                              workflow_name = workflow_name,
                              task_name = task_name) and self._tasks[task].num_samples() > 0,
                   self._tasks.keys())

    if len(tasks) == 0: return 0

    if len(tasks) > 1:
      raise WorkflowMeasurer_InternalError("Unexpectedly large number of samples for %s,%s,%s" % (
        job_uid, workflow_name, task_name))

    # ugh, need to memoize time.time()
    last_sample = self._tasks[tasks[0]].last_sample()
    if (time.time() - last_sample[0] < 10): # arbitrary time here
      return last_sample[1]
    else:
      return 0

  def samples_by_uid(self, job_uid):
    tasks = filter(lambda task: task.where(job_uid = job_uid), self._tasks.keys())
    return [self._tasks[task] for task in tasks]
