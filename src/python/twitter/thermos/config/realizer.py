import time
from pystachio import *
from twitter.thermos.config import schema
from twitter.thermos.config.loader import ThermosTaskWrapper

class ThermosTaskRealizer(object):
  class InvalidTaskError(ValueError): pass
  class InternalError(Exception): pass
  class RealizationError(Exception):
    def __init__(self, missing_info):
      Exception.__init__(self, "Not enough information to realize task: %s" % missing_info)

  def __init__(self, task, ports=None, task_id=None, user=None):
    assert isinstance(task, schema.Task)
    if not task.check().ok():
      raise ThermosTaskRealizer.InvalidTaskError(task.check().message())
    self._task = task
    self._task_id = task_id if task_id else '%s-%s' % (task.name(), time.strftime('%Y%m%d-%H%M%S'))
    self._user = user if user is not None else getpass.getuser()
    self._ports = {} or ports
    if not isinstance(self._ports, dict):
      raise ValueError("Ports must be a dictionary.")
    for portname in ThermosTaskWrapper(task).ports():
      if portname not in self._ports:
        raise ThermosTaskRealizer.RealizationError("Missing port %s" % portname)

  def context(self):
    return ThermosContext(ports   = self._ports,
                          task_id = self._task_id,
                          user    = self._user)

  def realized(self):
    interpolated = self._task % Environment(thermos = self.context())
    if not interpolated.check().ok():
      raise ThermosTaskRealizer.InternalError('Expected to realize task: %s' % (
        interpolated.check().message()))
    return interpolated
