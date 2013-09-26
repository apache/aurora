import threading

from .common.health_interface import FailureReason, HealthInterface

import mesos_pb2 as mesos_pb


class KillManager(HealthInterface):
  """
    A health interface that provides a kill-switch for a task monitored by the status manager.
  """
  def __init__(self):
    self._killed = False
    self._reason = None

  @property
  def healthy(self):
    return not self._killed

  @property
  def failure_reason(self):
    return FailureReason(self._reason, status=mesos_pb.TASK_KILLED)

  def kill(self, reason):
    self._reason = reason
    self._killed = True
