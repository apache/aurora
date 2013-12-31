from apache.aurora.client.api.scheduler_client import SchedulerProxy


class FakeSchedulerProxy(SchedulerProxy):
  def __init__(self, cluster, scheduler, session_key):
    self._cluster = cluster
    self._scheduler = scheduler
    self._session_key = session_key

  def client(self):
    return self._scheduler

  def session_key(self):
    return self._session_key
