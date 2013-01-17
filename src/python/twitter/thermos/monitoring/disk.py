"""Sample disk usage under a particular path

This module provides threads which can be used to gather information on the disk utilisation
under a particular path.

Currently, there are two threads available:
  - DiskCollectorThread, which periodically uses a basic brute-force approach (os.stat()ing every
    file within the path)
  - InotifyDiskCollectorThread, which updates disk utilisation dynamically by using inotify to
    monitor disk changes within the path

"""

import os
import threading
import time
from Queue import Empty, Queue

from twitter.common import log
from twitter.common.dirutil import du, safe_bsize
from twitter.common.exceptions import ExceptionalThread
from twitter.common.quantity import Amount, Time

from watchdog.observers import Observer as WatchdogObserver
from watchdog.events import (
  FileSystemEventHandler,
  FileCreatedEvent,
  FileDeletedEvent,
  FileModifiedEvent,
  FileMovedEvent,
)


class DiskCollectorThread(ExceptionalThread):
  """ Thread to calculate aggregate disk usage under a given path using a simple algorithm """
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


class DiskCollector(object):
  """ Spawn a background thread to sample disk usage """
  def __init__(self, root):
    self._root = root
    self._thread = None
    self._value = 0

  def sample(self):
    """ Trigger collection of sample, if not already begun """
    if self._thread is None:
      self._thread = DiskCollectorThread(self._root)
      self._thread.start()

  @property
  def value(self):
    """ Retrieve value of disk usage """
    if self._thread is not None and self._thread.finished():
      self._value = self._thread.value
      self._thread = None
    return self._value


class InotifyDiskCollectorThread(ExceptionalThread, FileSystemEventHandler):
  """ Thread to calculate aggregate disk usage under a given path

    Note that while this thread uses inotify (through the watchdog module) to monitor disk events in
    "real time", the actual processing of events is only performed periodically (configured via
    COLLECTION_INTERVAL)

  """
  INTERESTING_EVENTS = (FileCreatedEvent, FileDeletedEvent, FileModifiedEvent, FileMovedEvent)
  COLLECTION_INTERVAL = Amount(5, Time.SECONDS)

  def __init__(self, path):
    self._path = path
    self._files = {}   # file path => size (bytes)
    self._queue = Queue()
    self._observer = WatchdogObserver()
    threading.Thread.__init__(self)
    self.daemon = True

  def dispatch(self, event):
    """ Dispatch all interesting events to the internal queue """
    if isinstance(event, self.INTERESTING_EVENTS):
      self._queue.put(event)

  def _initialize(self):
    """ Collect an initial snapshot of the disk usage in the path """
    log.debug("Starting watchdog observer to collect events...")
    self._observer.schedule(self, path=self._path, recursive=True)
    self._observer.start()
    log.debug("Collecting initial disk usage sample...")
    for root, _, files in os.walk(self._path):
      for filename in files:
        f = os.path.join(root, filename)
        self._files[f] = safe_bsize(f)

  def _process_events(self):
    """ Deduplicate and process watchdog events, updating the internal file store appropriately """
    file_ops = {}

    def remove_file(path):
      self._files.pop(path, None)
    def stat_file(path):
      self._files[path] = safe_bsize(path)

    while not self._to_process.empty():
      event = self._to_process.get()
      if isinstance(event, (FileCreatedEvent, FileModifiedEvent)):
        file_ops[event.src_path] = lambda: stat_file(event.src_path)
      elif isinstance(event, FileDeletedEvent):
        file_ops[event.src_path] = lambda: remove_file(event.src_path)
      elif isinstance(event, FileMovedEvent):
        file_ops[event.src_path] = lambda: remove_file(event.src_path)
        file_ops[event.dest_path] = lambda: stat_file(event.dest_path)

    for op in file_ops.values():
      op()

  def run(self):
    """ Loop indefinitely, periodically processing watchdog/inotify events. """
    self._initialize()
    log.debug("Initialization complete. Moving to handling events.")
    while True:
      next = time.time() + self.COLLECTION_INTERVAL.as_(Time.SECONDS)
      if not self._queue.empty():
        self._to_process, self._queue = self._queue, Queue()
        self._process_events()
      time.sleep(max(0, next - time.time()))

  @property
  def value(self):
    return sum(self._files.itervalues())


class InotifyDiskCollector(object):
  """ Spawn a background thread to sample disk usage """
  def __init__(self, root):
    self._root = root
    self._thread = InotifyDiskCollectorThread(self._root)

  def sample(self):
    """ Trigger disk collection loop. """
    if not os.path.exists(self._root):
      log.error('Cannot start monitoring path until it exists')
    elif not self._thread.is_alive():
      self._thread.start()

  @property
  def value(self):
    return self._thread.value
