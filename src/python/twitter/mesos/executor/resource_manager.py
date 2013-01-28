import os
import threading
import time

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.quantity import Amount, Time
from twitter.thermos.monitoring.resource import TaskResourceMonitor

from gen.twitter.mesos.comm.ttypes import TaskResourceSample

from .health_interface import (
    FailureReason,
    HealthInterface)

import mesos_pb2 as mesos_pb
import psutil as ps


class ResourceEnforcer(object):
  """ Examine a task's resource consumption and determine whether it needs to be killed or
  adjusted """
  # TODO(jon): rework to not directly require psutil to inspect/adjust processes.
  NICE_INCREMENT = 5            # Increment nice by this amount
  NICE_BACKOFF   = 1            # Decrement nice by this amount
  NICE_BACKOFF_THRESHOLD = 0.8  # Decrement nice once cpu drops below this percentage of max

  PRIO_MIN       = -20          # POSIX nice/renice priority bands
  PRIO_NORMAL    = 0
  PRIO_MAX       = 20

  # On Linux, the maximum niceness for a process is actually 19 (even though PRIO_MAX is
  # defined as 20 at the kernel level).
  if os.uname()[0] == 'Linux':
    PRIO_MAX = 19

  ENFORCE_PORT_RANGE = (31000, 32000)

  def __init__(self, resources, task_monitor, portmap={}):
    """
      resources: Resources object specifying cpu, ram, disk limits for the task
      task_monitor: TaskMonitor exposing attributes about the task
    """
    self._max_cpu = resources.cpu().get()
    self._max_ram = resources.ram().get()
    self._max_disk = resources.disk().get()
    self._task_monitor = task_monitor
    self._portmap = dict((number, name) for (name, number) in portmap.items())
    self._nice = None

  @property
  def nice(self):
    """ Reflects the niceness of the processes in the task """
    return self._nice or self.PRIO_NORMAL

  def parents(self):
    """ List the current constituent Processes of the task (which may have child processes) """
    for process in self._task_monitor.get_active_processes():
      try:
        yield ps.Process(process[0].pid)
      except ps.error.Error as e:
        log.error('Error when collecting process %d: %s' % (process[0].pid, e))

  def walk(self):
    """ Generator yielding every Process in the task, including all of their children """
    for parent in self.parents():
      yield parent
      try:
        for child in parent.get_children(recursive=True):
          yield child
      except ps.error.Error as e:
        log.debug('Error when collecting process information: %s' % e)

  # TODO(wickman): Setting a uniform nice level might not be the intended
  # solution if the parents are already renicing subprocesses.  Should we just
  # add niceness rather than setting it?
  def _enforce_cpu(self, sample):
    nice_level = None
    for parent in self.parents():
      log.debug('Checking CPU rate for %s' % parent.pid)
      if sample.cpuRate > self._max_cpu and parent.nice < self.PRIO_MAX:
        log.debug('CPU rate too high - adjusting nice level')
        nice_level = min(self.PRIO_MAX, parent.nice + self.NICE_INCREMENT)
      elif sample.cpuRate < self._max_cpu * self.NICE_BACKOFF_THRESHOLD and (
          parent.nice != self.PRIO_NORMAL):
        nice_level = max(self.PRIO_NORMAL, parent.nice - self.NICE_BACKOFF)
      if nice_level is not None:
        log.debug('Adjusting nice level from %s=>%s' % (
          self._nice if self._nice is not None else parent.nice, nice_level))
        self._nice = nice_level
    if self._nice is not None:
      for process in self.walk():
        try:
          if process.nice != self._nice:
            process.nice = self._nice
        except ps.error.Error as e:
          log.error('Unable to adjust nice of %s: %s' % (process, e))

  def _enforce_ram(self, sample):
    if sample.ramRssBytes > self._max_ram:
      # TODO(wickman) Add human-readable resource ranging support to twitter.common.
      return FailureReason('RAM limit exceeded.  Reserved %s bytes vs resident %s bytes' % (
          self._max_ram, sample.ramRssBytes), status=mesos_pb.TASK_KILLED)

  def _enforce_disk(self, sample):
    if sample.diskBytes > self._max_disk:
      return FailureReason('Disk limit exceeded.  Reserved %s bytes vs used %s bytes.' % (
          self._max_disk, sample.diskBytes), status=mesos_pb.TASK_KILLED)

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
          return FailureReason('Listening on unallocated port %s.  Portmap is %s' % (
              port, self.render_portmap(self._portmap)), status=mesos_pb.TASK_KILLED)

  def enforce(self, sample):
    """
      Enforce resource constraints.

      Returns 'true' if the task should be killed, i.e. it has gone over its
      non-compressible resources:
         ram
         disk
    """
    # TODO(wickman) Due to MESOS-1585, port enforcement has been unwired.  If you would like to
    # add it back, simply add self._enforce_ports into the list of enforcers.
    for enforcer in (self._enforce_ram, self._enforce_disk, self._enforce_cpu):
      log.debug("Running enforcer: %s" % enforcer)
      kill_reason = enforcer(sample)
      if kill_reason:
        return kill_reason


class ResourceManager(HealthInterface, ExceptionalThread):
  """ Manage resources consumed by a Task """

  def __init__(self, resources, task_monitor, sandbox,
               enforcement_interval=Amount(30, Time.SECONDS)):
    """
      resources: Resources object specifying cpu, ram, disk limits for the task
      task_monitor: TaskMonitor exposing attributes about the task
      sandbox: the directory that we should monitor for disk usage
      enforcement_interval: how often resource enforcement should be conducted
    """
    self._resource_monitor = TaskResourceMonitor(task_monitor, sandbox)
    self._enforcer = ResourceEnforcer(resources, task_monitor)
    self._enforcement_interval = enforcement_interval.as_(Time.SECONDS)
    if self._enforcement_interval <= 0:
      raise ValueError('Resource enforcement interval must be >= 0')
    self._max_cpu = resources.cpu().get()
    self._max_ram = resources.ram().get()
    self._max_disk = resources.disk().get()
    self._kill_reason = None
    self._sample = None
    self._stop_event = threading.Event()
    ExceptionalThread.__init__(self)
    self.daemon = True

  # TODO(jon): clean this shit up
  @property
  def _num_procs(self):
    """ Total number of processes the task consists of (including child processes) """
    return self._resource_monitor.sample()[1].num_procs

  @property
  def _ps_sample(self):
    """ ProcessSample representing the aggregate resource consumption of the Task's processes """
    return self._resource_monitor.sample()[1].process_sample

  @property
  def _disk_sample(self):
    """ Integer in bytes representing the disk consumption in the Task's sandbox """
    return self._resource_monitor.sample()[1].disk_usage

  @property
  def sample(self):
    """ Return a TaskResourceSample representing the total resource consumption of the Task """
    # TODO(jon): switch this to using ResourceResult (or similar) instead, push TaskResourceSample
    # into the checkpointer only
    now = time.time()
    return TaskResourceSample(
        microTimestamp=int(now * 1e6),
        reservedCpuRate=self._max_cpu,
        reservedRamBytes=self._max_ram,
        reservedDiskBytes=self._max_disk,
        cpuRate=self._ps_sample.rate,
        cpuUserSecs=self._ps_sample.user,
        cpuSystemSecs=self._ps_sample.system,
        cpuNice=self._enforcer.nice,
        ramRssBytes=self._ps_sample.rss,
        ramVssBytes=self._ps_sample.vms,
        numThreads=self._ps_sample.threads,
        numProcesses=self._num_procs,
        diskBytes=self._disk_sample
    )

  @property
  def healthy(self):
    return self._kill_reason is None

  @property
  def failure_reason(self):
    return self._kill_reason

  def run(self):
    """ Periodically conduct enforcement. Resources are aggregated in the background thread of the
    ResourceMonitor. """

    self._resource_monitor.start()

    while not self._stop_event.is_set():
      # TODO(jon): pass enforcer ProcessSample et al instead; it doesn't need TaskResourceSample
      kill_reason = self._enforcer.enforce(self.sample)
      if kill_reason and not self._kill_reason:
        log.info('ResourceManager triggering kill - reason: %s' % kill_reason)
        self._kill_reason = kill_reason
      self._stop_event.wait(timeout=self._enforcement_interval)

  def start(self):
    ExceptionalThread.start(self)

  def stop(self):
    self._stop_event.set()
