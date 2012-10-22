from collections import namedtuple, defaultdict
from functools import partial
from psutil import Process, get_pid_list
from psutil.error import NoSuchProcess, AccessDenied
import time
import threading

from twitter.common import log
from twitter.common.quantity import Time, Amount

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False


_ProcessSample = namedtuple('_ProcessSample', 'rate user system rss vms nice status threads')
class ProcessSample(_ProcessSample):
  @staticmethod
  def get(pid_or_process, with_children=True, memoized={}):
    if isinstance(pid_or_process, int):
      process = Process(pid_or_process)
    else:
      process = pid_or_process
    if not with_children:
      return ProcessSample.process_to_sample(process, memoized)
    else:
      return sum(map(partial(ProcessSample.process_to_sample, memoized=memoized),
        process.get_children()), ProcessSample.process_to_sample(process, memoized))

  @staticmethod
  def process_to_sample(process, memoized={}):
    try:
      rate = process.get_cpu_percent(0.0) / 100.0
      user, system = process.get_cpu_times()
      rss, vms = process.get_memory_info()
      nice = process.nice
      status = process.status
      threads = process.get_num_threads()
      return ProcessSample(rate, user, system, rss, vms, nice, status, threads)
    except (AccessDenied, NoSuchProcess):
      return ProcessSample.empty()

  @staticmethod
  def empty():
    return ProcessSample(rate=0, user=0, system=0, rss=0, vms=0, nice=None, status=None, threads=0)

  @staticmethod
  def aggregate(samples):
    if len(samples) == 0:
      return ProcessSample.empty()

    def average(values):
      return sum(map(float, values)) / len(values) if len(values) > 0 else 0

    def mode(values):
      if len(values) == 0:
        return None
      elif len(values) == 1:
        return values[0]
      else:
        agg = defaultdict(int)
        for value in values: agg[value] += 1
        return sorted(agg.items(), key=lambda x: x[1])[-1][0]

    rate    = average([sample.rate for sample in samples])
    user    = average([sample.user for sample in samples])
    system  = average([sample.system for sample in samples])
    rss     = max([sample.rss for sample in samples])
    vms     = max([sample.vms for sample in samples])
    nice    = average(filter(None, [sample.nice for sample in samples]))
    status  = mode(filter(None, [sample.status for sample in samples]))
    threads = int(average([sample.threads for sample in samples]))
    return ProcessSample(rate, user, system, rss, vms, nice, status, threads)

  def __add__(self, other):
    if self.nice is not None and other.nice is None:
      nice = self.nice
    else:
      nice = other.nice
    if self.status is not None and other.status is None:
      status = self.status
    else:
      status = other.status
    return ProcessSample(
      rate = self.rate + other.rate,
      user = self.user + other.user,
      system = self.system + other.system,
      rss = self.rss + other.rss,
      vms = self.vms + other.vms,
      nice = nice,
      status = status,
      threads = self.threads + other.threads)

  def to_dict(self):
    return dict(
      cpu     = self.rate,
      ram     = self.rss,
      user    = self.user,
      system  = self.system,
      rss     = self.rss,
      vms     = self.vms,
      nice    = self.nice,
      status  = str(self.status),
      threads = self.threads
    )


class ProcessState(object):
  def __init__(self):
    self._processes = {}   # pid => Process
    self._samples = {}
    self._pidmap = defaultdict(set)

  def update(self):
    # update processes with live processes
    pids = set(filter(None, get_pid_list()))
    last_pids = set(self._processes.keys())
    for pid in last_pids - pids:
      self._processes.pop(pid)
    for pid in pids - last_pids:
      try:
        process = Process(pid)
      except NoSuchProcess:
        continue
      self._processes[pid] = process
    self._samples = {}
    self._pidmap = defaultdict(set)
    for pid, process in self._processes.items():
      try:
        self._pidmap[process.ppid].add(pid)
      except NoSuchProcess:
        continue

  def sample(self, roots=None):
    self._samples = {}
    if roots is None:
      tree = set(self._processes.keys())
    else:
      tree = set(roots)
      for pid in tree.copy():
        tree.update(self.children_of(pid))
    for pid in tree:
      if pid not in self._processes:
        log.warning("Attempting to sample pid %s but it's not in the list of active processes" %
          pid)
        continue
      try:
        self._samples[pid] = ProcessSample.process_to_sample(self._processes[pid])
      except (NoSuchProcess, AccessDenied):
        continue

  def children_of(self, pid):
    pidset = set(self._pidmap.get(pid, []))
    for cpid in self._pidmap.get(pid, []):
      pidset.update(self.children_of(cpid))
    return pidset

  def aggregate(self, pid):
    if pid not in self._samples:
      log.warning('Called aggregate on an inactive task.')
      return ProcessSample.empty()
    log.debug('Aggregating %s => %s' % (pid, self.children_of(pid)))
    log.debug('   pid (%s) => %s' % (pid, self._samples.get(pid)))
    for cpid in self.children_of(pid):
      log.debug('     cpid (%s) => %s' % (cpid, self._samples.get(cpid)))
    return sum(filter(None, map(self._samples.get, self.children_of(pid))),
      self._samples.get(pid, ProcessSample.empty()))


class TaskMeasurer(threading.Thread):
  """
    Class responsible for polling CPU/RAM/DISK usage from live processes.
    Sublcassed from thread, runs in a background thread.  Control with start()/join().
  """

  class InternalError(Exception): pass
  SAMPLE_INTERVAL = Amount(10, Time.SECONDS)

  def __init__(self, muxer, interval = SAMPLE_INTERVAL):
    self._muxer = muxer
    self._kill_signal = threading.Event()
    if not isinstance(interval, Amount):
      raise ValueError('Expected interval to be a Time Amount.')
    self._interval = interval.as_(Time.SECONDS)
    self._lock = threading.Lock()
    self._ps = ProcessState()
    self._actives = set()
    threading.Thread.__init__(self)
    self.daemon = True

  def join(self):
    self._kill_signal.set()
    threading.Thread.join(self)

  @staticmethod
  def sleep_until(timestamp):
    time_now = time.time()
    total_sleep = timestamp - time_now
    if total_sleep > 0:
      time.sleep(total_sleep)

  def run(self):
    start = time.time()
    time_now = start

    while not self._kill_signal.is_set():
      last_interval = time_now
      TaskMeasurer.sleep_until(last_interval + self._interval)
      time_now = time.time()
      with self._lock:
        self._actives = set(self._muxer.get_active_processes())
        self._ps.update()
        self._ps.sample(process.pid for _, process, _ in self._actives)

  def sample_by_task_id(self, task_id):
    with self._lock:
      samples = [
          self._ps.aggregate(sample.pid) for tid, sample, _ in self._actives if tid == task_id]
      return sum(samples, ProcessSample.empty())

  def sample_by_process(self, task_id, process_name):
    with self._lock:
      samples = [self._ps.aggregate(sample.pid) for tid, sample, _ in self._actives
                 if tid == task_id and sample.process == process_name]
      return sum(samples, ProcessSample.empty())
