import time

from twitter.common import log
from twitter.common.quantity import Amount, Time

from twitter.aurora.client.api import AuroraClientAPI
from twitter.aurora.client.base import check_and_log_response
from twitter.servermaint.batching import Batching

from gen.twitter.aurora.ttypes import Hosts, MaintenanceMode


class MesosMaintenance(object):
  """This class provides more methods to interact with the mesos cluster and perform
  maintenance.
  """

  START_MAINTENANCE_DELAY = Amount(30, Time.SECONDS)

  def __init__(self, cluster, verbosity):
    self._client = AuroraClientAPI(cluster, verbosity == 'verbose')

  def _drain_hosts(self, drainable_hosts, clock=time):
    """This will actively turn down tasks running on hosts."""
    check_and_log_response(self._client.drain_hosts(drainable_hosts))
    not_ready_hosts = [hostname for hostname in drainable_hosts.hostNames]
    while not_ready_hosts:
      log.info("Sleeping for %s." % self.START_MAINTENANCE_DELAY)
      clock.sleep(self.START_MAINTENANCE_DELAY.as_(Time.SECONDS))
      resp = self._client.maintenance_status(Hosts(not_ready_hosts))
      #TODO(jsmith): Workaround until scheduler responds with unknown slaves in MESOS-3454
      if not resp.result.maintenanceStatusResult.statuses:
        not_ready_hosts = None
      for host_status in resp.result.maintenanceStatusResult.statuses:
        if host_status.mode != MaintenanceMode.DRAINED:
          log.warning('%s is currently in status %s' %
              (host_status.host, MaintenanceMode._VALUES_TO_NAMES[host_status.mode]))
        else:
          not_ready_hosts.remove(host_status.host)

  def _complete_maintenance(self, drained_hosts):
    """End the maintenance status for a give set of hosts."""
    check_and_log_response(self._client.end_maintenance(drained_hosts))
    resp = self._client.maintenance_status(drained_hosts)
    for host_status in resp.result.maintenanceStatusResult.statuses:
      if host_status.mode != MaintenanceMode.NONE:
        log.warning('%s is DRAINING or in DRAINED' % host_status.host)

  def _operate_on_hosts(self, drained_hosts, callback):
    """Perform a given operation on a list of hosts that are ready for maintenance."""
    for host in drained_hosts.hostNames:
      callback(host)

  def end_maintenance(self, hosts):
    """Pull a list of hosts out of maintenance mode."""
    self._complete_maintenance(Hosts(set(hosts)))

  def start_maintenance(self, hosts):
    """Put a list of hosts into maintenance mode, to de-prioritize scheduling."""
    check_and_log_response(self._client.start_maintenance(Hosts(set(hosts))))

  def perform_maintenance(self, hosts, batch_size=0, callback=None):
    """The wrap a callback in between sending hosts into maintenance mode and back.

    Walk through the process of putting hosts into maintenance, draining them of tasks,
    performing an action on them once drained, then removing them from maintenance mode
    so tasks can schedule.
    """
    self._complete_maintenance(Hosts(set(hosts)))
    self.start_maintenance(hosts)
    batcher = Batching()
    if batch_size > 0:
      hosts = batcher.batch_hosts_by_size(hosts, batch_size)
    else:
      hosts = batcher.batch_hosts_by_rack(hosts)
    for batch in hosts:
      drainable_hosts = Hosts(set(batch))
      self._drain_hosts(drainable_hosts)
      if callback:
        self._operate_on_hosts(drainable_hosts, callback)
      self._complete_maintenance(drainable_hosts)

  def check_status(self, hosts):
    resp = self._client.maintenance_status(Hosts(set(hosts)))
    check_and_log_response(resp)
    statuses = []
    for host_status in resp.result.maintenanceStatusResult.statuses:
      statuses.append((host_status.host, MaintenanceMode._VALUES_TO_NAMES[host_status.mode]))
    return statuses
