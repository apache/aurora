""" Sample disk usage under a particular path """

import threading

from twitter.common.dirutil import du

class DiskCollectorThread(threading.Thread):
  """ Thread to calculate aggregate disk usage under a given path """
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
    if self._thread is not None and self._thread.finished():
      self._value = self._thread.value
      self._thread = None
    return self._value
