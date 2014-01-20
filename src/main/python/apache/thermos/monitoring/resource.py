#
# Copyright 2013 Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Monitor the resource consumption of Thermos tasks

This module contains classes used to monitor the resource consumption (e.g. CPU, RAM, disk) of
Thermos tasks. Resource monitoring of a Thermos task typically treats the task as an aggregate of
all the processes within it. Importantly, this excludes the process(es) of Thermos itself (i.e. the
TaskRunner and any other wrappers involved in launching a task).

The ResourceMonitorBase defines the interface for other components (for example, the Thermos
TaskObserver) to interact with and retrieve information about a Task's resource consumption.  The
canonical/reference implementation of a ResourceMonitor is the TaskResourceMonitor, a thread which
actively monitors resources for a particular task by periodically polling process information and
disk consumption and retaining a limited (FIFO) in-memory history of this data.

"""

from abc import abstractmethod
from bisect import bisect_left
from collections import namedtuple
from operator import attrgetter
import platform
import threading
import time

from twitter.common import log
from twitter.common.collections import RingBuffer
from twitter.common.concurrent import EventMuxer
from twitter.common.lang import Interface
from twitter.common.quantity import Amount, Time

from .disk import DiskCollector
from .monitor import TaskMonitor
from .process import ProcessSample
from .process_collector_psutil import ProcessTreeCollector


class ResourceMonitorBase(Interface):
  """ Defines the interface for interacting with a ResourceMonitor """

  class Error(Exception): pass

  class ResourceResult(namedtuple('ResourceResult', 'num_procs process_sample disk_usage')):
    pass

  @abstractmethod
  def sample(self):
    """ Return a sample of the resource consumption of the task right now

    Returns a tuple of (timestamp, ResourceResult)
    """

  @abstractmethod
  def sample_at(self, time):
    """ Return a sample of the resource consumption as close as possible to the specified time

    Returns a tuple of (timestamp, ResourceResult)
    """

  @abstractmethod
  def sample_by_process(self, process_name):
    """ Return a sample of the resource consumption of a specific process in the task right now

    Returns a ProcessSample
    """


class ResourceHistory(object):
  """Simple class to contain a RingBuffer (fixed-length FIFO) history of resource samples, with the
       mapping: timestamp => (number_of_procs, ProcessSample, disk_usage_in_bytes)
  """
  def __init__(self, maxlen, initialize=True):
    if not maxlen >= 1:
      raise ValueError("maxlen must be greater than 0")
    self._maxlen = maxlen
    self._values = RingBuffer(maxlen, None)
    if initialize:
      self.add(time.time(), ResourceMonitorBase.ResourceResult(0, ProcessSample.empty(), 0))

  def add(self, timestamp, value):
    """Store a new resource sample corresponding to the given timestamp"""
    if self._values and not timestamp >= self._values[-1][0]:
      raise ValueError("Refusing to add timestamp in the past!")
    self._values.append((timestamp, value))

  def get(self, timestamp):
    """Get the resource sample nearest to the given timestamp"""
    closest = min(bisect_left(self._values, (timestamp, None)), len(self) - 1)
    return self._values[closest]

  def __iter__(self):
    return iter(self._values)

  def __len__(self):
    return len(self._values)

  def __repr__(self):
    return 'ResourceHistory(%s)' % ', '.join([str(r) for r in self._values])


class TaskResourceMonitor(ResourceMonitorBase, threading.Thread):
  """ Lightweight thread to aggregate resource consumption for a task's constituent processes.
      Actual resource calculation is delegated to collectors; this class periodically polls the
      collectors and aggregates into a representation for the entire task. Also maintains a limited
      history of previous sample results.
  """

  MAX_HISTORY = 10000 # magic number

  def __init__(self, task_monitor, sandbox,
               process_collector=ProcessTreeCollector, disk_collector=DiskCollector,
               process_collection_interval=Amount(20, Time.SECONDS),
               disk_collection_interval=Amount(1, Time.MINUTES),
               history_time=Amount(1, Time.HOURS)):
    """
      task_monitor: TaskMonitor object specifying the task whose resources should be monitored
      sandbox: Directory for which to monitor disk utilisation
    """
    self._task_monitor = task_monitor # exposes PIDs, sandbox
    self._task_id = task_monitor._task_id
    log.debug('Initialising resource collection for task %s' % self._task_id)
    self._process_collectors = dict() # ProcessStatus => ProcessTreeCollector
    # TODO(jon): sandbox is also available through task_monitor, but typically the first checkpoint
    # isn't written (and hence the header is not available) by the time we initialise here
    self._sandbox = sandbox
    self._process_collector_factory = process_collector
    self._disk_collector = disk_collector(self._sandbox)
    self._process_collection_interval = process_collection_interval.as_(Time.SECONDS)
    self._disk_collection_interval = disk_collection_interval.as_(Time.SECONDS)
    min_collection_interval = min(self._process_collection_interval, self._disk_collection_interval)
    history_length = int(history_time.as_(Time.SECONDS) / min_collection_interval)
    if history_length > self.MAX_HISTORY:
      raise ValueError("Requested history length too large")
    log.debug("Initialising ResourceHistory of length %s" % history_length)
    self._history = ResourceHistory(history_length)
    self._kill_signal = threading.Event()
    threading.Thread.__init__(self)
    self.daemon = True

  def sample(self):
    if not self.is_alive():
      log.warning("TaskResourceMonitor not running - sample may be inaccurate")
    return self.sample_at(time.time())

  def sample_at(self, timestamp):
    return self._history.get(timestamp)

  def sample_by_process(self, process_name):
    try:
      process = [process for process in self._get_active_processes()
                 if process.process == process_name].pop()
    except IndexError:
      raise ValueError('No active process found with name "%s" in this task' % process_name)
    else:
      # Since this might be called out of band (before the main loop is aware of the process)
      if process not in self._process_collectors:
        self._process_collectors[process] = self._process_collector_factory(process.pid)

      self._process_collectors[process].sample()
      return self._process_collectors[process].value

  def _get_active_processes(self):
    """Get a list of ProcessStatus objects representing currently-running processes in the task"""
    return [process for process, _ in self._task_monitor.get_active_processes()]

  def kill(self):
    """Signal that the thread should cease collecting resources and terminate"""
    self._kill_signal.set()

  def run(self):
    """Thread entrypoint. Loop indefinitely, polling collectors at self._collection_interval and
    collating samples."""

    log.debug('Commencing resource monitoring for task "%s"' % self._task_id)
    next_process_collection = 0
    next_disk_collection = 0

    while not self._kill_signal.is_set():

      now = time.time()

      if now > next_process_collection:
        next_process_collection = now + self._process_collection_interval
        actives = set(self._get_active_processes())
        current = set(self._process_collectors)
        for process in current - actives:
          log.debug('Process "%s" (pid %s) no longer active, removing from monitored processes' %
                   (process.process, process.pid))
          self._process_collectors.pop(process)
        for process in actives - current:
          log.debug('Adding process "%s" (pid %s) to resource monitoring' %
                   (process.process, process.pid))
          self._process_collectors[process] = self._process_collector_factory(process.pid)
        for process, collector in self._process_collectors.iteritems():
          log.debug('Collecting sample for process "%s" (pid %s) and children' %
                   (process.process, process.pid))
          collector.sample()

      if now > next_disk_collection:
        next_disk_collection = now + self._disk_collection_interval
        log.debug('Collecting disk sample for %s' % self._sandbox)
        self._disk_collector.sample()

      try:
        aggregated_procs = sum(map(attrgetter('procs'), self._process_collectors.values()))
        aggregated_sample = sum(map(attrgetter('value'), self._process_collectors.values()),
                                ProcessSample.empty())
        self._history.add(now, self.ResourceResult(aggregated_procs, aggregated_sample,
                                                   self._disk_collector.value))
        log.debug("Recorded resource sample at %s" % now)
      except ValueError as err:
        log.warning("Error recording resource sample: %s" % err)

      # Sleep until any of the following conditions are met:
      # - it's time for the next disk collection
      # - it's time for the next process collection
      # - the result from the last disk collection is available via the DiskCollector
      # - the TaskResourceMonitor has been killed via self._kill_signal
      now = time.time()
      next_collection = min(next_process_collection - now, next_disk_collection - now)
      EventMuxer(self._kill_signal, self._disk_collector.completed_event
                ).wait(timeout=max(0, next_collection))

    log.debug('Stopping resource monitoring for task "%s"' % self._task_id)
