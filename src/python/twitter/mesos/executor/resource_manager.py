import os
import threading
import time

import psutil as ps
from thrift.TSerialization import serialize as thrift_serialize

from twitter.common import log
from twitter.common.dirutil import du
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordWriter
from twitter.thermos.monitoring.measure import ProcessSample

from gen.twitter.mesos.comm.ttypes import TaskResourceSample


class DiskCollectorThread(threading.Thread):
  def __init__(self, path):
    self.path = path
    self.value = None
    self.event = threading.Event()
    threading.Thread.__init__(self)
    self.daemon = True

  def run(self):
    self.value = du(self.path)
    self.event.set()

  def finished(self):
    return self.event.is_set()


class DiskCollection(object):
  def __init__(self, root):
    self._root = root
    self._thread = None
    self._value = 0

  def sample(self):
    if self._thread is None:
      self._thread = DiskCollectorThread(self._root)
      self._thread.start()

  @property
  def value(self):
    if self._thread is not None and self._thread.finished():
      self._value = self._thread.value
      self._thread = None
    return self._value


class ProcessCollection(object):
  @classmethod
  def collect(cls, pid):
    try:
      parent_process = ps.Process(pid)
      parent = ProcessSample.process_to_sample(parent_process)
      children = map(ProcessSample.process_to_sample, parent_process.get_children(recursive=True))
      return len(children) + 1, sum(children, parent)
    except ps.error.Error as e:
      log.warning('Error during process sampling: %s' % e)
      return 0, ProcessSample.empty()

  def __init__(self, pid):
    self._pid = pid
    self._sample = None
    self._stamp = None
    self._rate = 0.0
    self._procs = 1

  def sample(self):
    last_sample, last_stamp = self._sample, self._stamp
    self._procs, self._sample = self.collect(self._pid)
    self._stamp = time.time()

    # the rate calculation appears to be broken, fix it here for now
    if last_sample and last_stamp:
      self._rate = ((self._sample.user + self._sample.system) - (
        last_sample.user + last_sample.system)) / (self._stamp - last_stamp)

  @property
  def value(self):
    return self._sample

  @property
  def rate(self):
    return self._rate

  @property
  def procs(self):
    return self._procs


class ResourceEnforcer(object):
  NICE_INCREMENT = 5            # Increment nice by this amount
  NICE_BACKOFF   = 1            # Decrement nice by this amount
  NICE_BACKOFF_THRESHOLD = 0.8  # Decrement nice once cpu drops below this percentage of max

  PRIO_MIN       = -20          # POSIX nice/renice priority bands
  PRIO_NORMAL    = 0
  PRIO_MAX       = 20

  ENFORCE_PORT_RANGE = (31000, 32000)

  class KillReason(object):
    def __init__(self, reason):
      self._reason = reason

    @property
    def reason(self):
      return self._reason

    def __repr__(self):
      return 'KillReason(%r)' % self._reason

  def __init__(self, pid, portmap={}):
    self._pid = pid
    self._portmap = dict((number, name) for (name, number) in portmap.items())
    self._nice = None

  def parent(self):
    return ps.Process(self._pid)

  @property
  def nice(self):
    return self._nice or self.PRIO_NORMAL

  def walk(self):
    try:
      parent = self.parent()
      for child in parent.get_children(recursive=True):
        yield child
      yield parent
    except ps.error.Error as e:
      log.debug('Error when collecting process information: %s' % e)

  # TODO(wickman): Setting a uniform nice level might not be the intended
  # solution if the parent is already renicing subprocesses.  Should we just
  # add niceness rather than setting it?
  def _enforce_cpu(self, record):
    nice_level = None
    try:
      parent = self.parent()
    except ps.error.Error as e:
      log.debug('Unable to collect information about the parent: %s' % e)
      return
    if record.cpuRate > record.reservedCpuRate and parent.nice < self.PRIO_MAX:
      nice_level = min(self.PRIO_MAX, parent.nice + self.NICE_INCREMENT)
    elif record.cpuRate < record.reservedCpuRate * self.NICE_BACKOFF_THRESHOLD and (
        parent.nice != self.PRIO_NORMAL):
      nice_level = max(self.PRIO_NORMAL, parent.nice - self.NICE_BACKOFF)
    if nice_level is not None:
      log.debug('Adjusting nice level from %s=>%s' % (
        self._nice if self._nice is not None else parent.nice, nice_level))
      self._nice = nice_level
    if self._nice is not None:
      for process in self.walk():
        if process.nice != self._nice:
          process.nice = self._nice

  def _enforce_ram(self, record):
    if record.ramRssBytes > record.reservedRamBytes:
      # TODO(wickman) Add human-readable resource ranging support to twitter.common.
      return self.KillReason('RAM limit exceeded.  Reserved %s bytes vs resident %s bytes' % (
          record.reservedRamBytes, record.ramRssBytes))

  def _enforce_disk(self, record):
    if record.diskBytes > record.reservedDiskBytes:
      return self.KillReason('Disk limit exceeded.  Reserved %s bytes vs used %s bytes.' % (
          record.reservedDiskBytes, record.diskBytes))

  @staticmethod
  def render_portmap(portmap):
    return '{%s}' % (', '.join('%s=>%s' % (name, port) for name, port in portmap.items()))

  def get_listening_ports(self):
    for process in self.walk():
      try:
        for connection in process.get_connections():
          if connection.status == 'LISTEN':
            _, port = connection.local_address
            yield port
      except ps.error.Error as e:
        log.debug('Unable to collect information about %s: %s' % (process, e))

  def _enforce_ports(self, _):
    for port in self.get_listening_ports():
      if self.ENFORCE_PORT_RANGE[0] <= port <= self.ENFORCE_PORT_RANGE[1]:
        if port not in self._portmap:
          return self.KillReason('Listening on unallocated port %s.  Portmap is %s' % (
              port, self.render_portmap(self._portmap)))

  def enforce(self, record):
    """
      Enforce resource constraints.

      Returns 'true' if the task should be killed, i.e. it has gone over its
      non-compressible resources:
         ram
         disk
    """
    for enforcer in (self._enforce_ram, self._enforce_disk, self._enforce_ports, self._enforce_cpu):
      kill_reason = enforcer(record)
      if kill_reason:
        return kill_reason


class ResourceManager(threading.Thread):
  PROCESS_COLLECTION_INTERVAL = Amount(20, Time.SECONDS)
  DISK_COLLECTION_INTERVAL = Amount(1, Time.MINUTES)
  ENFORCEMENT_INTERVAL = Amount(30, Time.SECONDS)

  def __init__(self, resources, portmap, pid, sandbox):
    """
      resources: Resources object (cpu, ram, disk limits)
      portmap: dict { string => integer }
      pid: pid of the root in which to measure (presumably task runner)
      sandbox: the directory that we should monitor for disk usage
    """
    self._ps = ProcessCollection(pid)
    self._du = DiskCollection(sandbox)
    self._enforcer = ResourceEnforcer(pid, portmap)
    self._max_cpu = resources.cpu().get()
    self._max_ram = resources.ram().get()
    self._max_disk = resources.disk().get()
    self._kill_reason = None
    self._sample = None
    threading.Thread.__init__(self)
    self.daemon = True

  @property
  def sample(self):
    return self._sample

  def _collect_sample(self, now):
    sample = self._ps.value
    self._sample = TaskResourceSample(
        microTimestamp=int(now * 1e6),
        reservedCpuRate=self._max_cpu,
        reservedRamBytes=self._max_ram,
        reservedDiskBytes=self._max_disk,
        cpuRate=self._ps.rate,
        cpuUserSecs=sample.user,
        cpuSystemSecs=sample.system,
        cpuNice=self._enforcer.nice,
        ramRssBytes=sample.rss,
        ramVssBytes=sample.vms,
        numThreads=sample.threads,
        numProcesses=self._ps.procs,
        diskBytes=self._du.value)

  @property
  def kill_reason(self):
    return self._kill_reason

  def run(self):
    now = time.time()
    next_process_collection = now
    next_disk_collection = now
    next_enforcement = now

    while True:
      now = time.time()
      next_event = max(
          0, min(next_enforcement, next_process_collection, next_disk_collection) - now)
      time.sleep(next_event)

      now = time.time()
      if now > next_process_collection:
        next_process_collection = now + self.PROCESS_COLLECTION_INTERVAL.as_(Time.SECONDS)
        self._ps.sample()
        log.debug('Sampled CPU percentage: %.2f' % (100. * self._ps.rate))
      if now > next_disk_collection:
        next_disk_collection = now + self.DISK_COLLECTION_INTERVAL.as_(Time.SECONDS)
        self._du.sample()
        log.debug('Sampled disk bytes: %.1fMB' % (self._du.value / 1024. / 1024.))
      if now > next_enforcement:
        self._collect_sample(now)
        next_enforcement = now + self.ENFORCEMENT_INTERVAL.as_(Time.SECONDS)
        kill_reason = self._enforcer.enforce(self.sample)
        if kill_reason and not self._kill_reason:
          log.info('Resource manager triggering kill reason: %s' % kill_reason)
          self._kill_reason = kill_reason


class ResourceCheckpointer(threading.Thread):
  COLLECTION_INTERVAL = Amount(1, Time.MINUTES)

  @staticmethod
  def recordio_writer(checkpoint):
    trw = ThriftRecordWriter(open(checkpoint, 'wb'))
    trw.set_sync(True)
    return trw

  @staticmethod
  def static_writer(checkpoint):
    class Writer(object):
      def write(self, record):
        with open(checkpoint + '~', 'wb') as fp:
          fp.write(thrift_serialize(record))
        os.rename(checkpoint + '~', checkpoint)
    return Writer()

  def __init__(self, sample_provider, filename, recordio=False):
    self._sample_provider = sample_provider
    self._output = self.recordio_writer(filename) if recordio else self.static_writer(filename)
    threading.Thread.__init__(self)
    self.daemon = True

  def run(self):
    while True:
      time.sleep(self.COLLECTION_INTERVAL.as_(Time.SECONDS))
      sample = self._sample_provider()
      if sample:
        self._output.write(sample)
