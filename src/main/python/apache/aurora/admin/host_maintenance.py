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
from threading import Event

from twitter.common import log
from twitter.common.quantity import Amount, Time

from apache.aurora.admin.admin_util import format_sla_results, print_results
from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.base import DEFAULT_GROUPING, check_and_log_response, group_hosts

from gen.apache.aurora.api.ttypes import Hosts, MaintenanceMode


class HostMaintenance(object):
  """Submit requests to the scheduler to put hosts into and out of maintenance
  mode so they can be operated upon without causing LOST tasks.

  Aurora provides a two-tiered concept of Maintenance. The first step is to initiate maintenance,
  which will ask the Aurora scheduler to de-prioritize scheduling on a large set of hosts (the ones
  that will be operated upon during this maintenance window).  Once all hosts have been tagged in
  this manner, the operator can begin draining individual machines, which will have all user-tasks
  killed and rescheduled.  When the tasks get placed onto a new machine, the scheduler will first
  look for hosts that do not have the maintenance tag, which will help decrease churn and prevent a
  task from being constantly killed as its hosts go down from underneath it.
  """

  SLA_MIN_JOB_INSTANCE_COUNT = 20
  STATUS_POLL_INTERVAL = Amount(5, Time.SECONDS)
  MAX_STATUS_WAIT = Amount(5, Time.MINUTES)

  @classmethod
  def iter_batches(cls, hostnames, grouping_function=DEFAULT_GROUPING):
    groups = group_hosts(hostnames, grouping_function)
    groups = sorted(groups.items(), key=lambda v: v[0])
    for group in groups:
      yield Hosts(group[1])

  def __init__(self, cluster, verbosity, wait_event=None):
    self._client = AuroraClientAPI(cluster, verbosity == 'verbose')
    self._wait_event = wait_event or Event()

  def _drain_hosts(self, drainable_hosts):
    """"Drains tasks from the specified hosts.

    This will move active tasks on these hosts to the DRAINING state, causing them to be
    rescheduled elsewhere.

    :param drainable_hosts: Hosts that are in maintenance mode and ready to be drained
    :type drainable_hosts: gen.apache.aurora.ttypes.Hosts
    :rtype: set of host names failed to drain
    """
    check_and_log_response(self._client.drain_hosts(drainable_hosts))
    drainable_hostnames = [hostname for hostname in drainable_hosts.hostNames]

    total_wait = self.STATUS_POLL_INTERVAL
    not_drained_hostnames = set(drainable_hostnames)
    while not self._wait_event.is_set() and not_drained_hostnames:
      log.info('Waiting for hosts to be in DRAINED: %s' % not_drained_hostnames)
      self._wait_event.wait(self.STATUS_POLL_INTERVAL.as_(Time.SECONDS))

      statuses = self.check_status(list(not_drained_hostnames))
      not_drained_hostnames = set(h[0] for h in statuses if h[1] != 'DRAINED')

      total_wait += self.STATUS_POLL_INTERVAL
      if not_drained_hostnames and total_wait > self.MAX_STATUS_WAIT:
        log.warning('Failed to move all hosts into DRAINED within %s:\n%s' %
            (self.MAX_STATUS_WAIT,
            '\n'.join("\tHost:%s\tStatus:%s" % h for h in sorted(statuses) if h[1] != 'DRAINED')))
        break

    return not_drained_hostnames

  def _complete_maintenance(self, drained_hosts):
    """End the maintenance status for a given set of hosts.

    :param drained_hosts: Hosts that are drained and finished being operated upon
    :type drained_hosts: gen.apache.aurora.ttypes.Hosts
    """
    check_and_log_response(self._client.end_maintenance(drained_hosts))
    resp = self._client.maintenance_status(drained_hosts)
    for host_status in resp.result.maintenanceStatusResult.statuses:
      if host_status.mode != MaintenanceMode.NONE:
        log.warning('%s is DRAINING or in DRAINED' % host_status.host)

  def _check_sla(self, hostnames, grouping_function, percentage, duration):
    """Check if the provided list of hosts passes the job uptime SLA check.

    This is an all-or-nothing check, meaning that all provided hosts must pass their job
    SLA check for the maintenance to proceed.

    :param hostnames: list of host names to check SLA for
    :type hostnames: list of strings
    :param grouping_function: grouping function to apply to the given hosts
    :type grouping_function: function
    :param percentage: SLA uptime percentage override
    :type percentage: float
    :param duration: SLA uptime duration override
    :type duration: twitter.common.quantity.Amount
    :rtype: set of unsafe hosts
    """
    vector = self._client.sla_get_safe_domain_vector(self.SLA_MIN_JOB_INSTANCE_COUNT, hostnames)
    host_groups = vector.probe_hosts(
      percentage,
      duration.as_(Time.SECONDS),
      grouping_function)

    unsafe_hostnames = set()
    # Given that maintenance is performed 1 group at a time, any result longer than 1 group
    # should be considered a batch failure.
    if host_groups:
      if len(host_groups) > 1:
        log.error('Illegal multiple groups detected in SLA results. Skipping hosts: %s' % hostnames)
        return set(hostnames)

      results, unsafe_hostnames = format_sla_results(host_groups, unsafe_only=True)
      if results:
        print_results(results)
        return unsafe_hostnames

    return unsafe_hostnames

  def end_maintenance(self, hostnames):
    """Pull a list of hostnames out of maintenance mode.

    :param hostnames: List of hosts to operate upon
    :type hostnames: list of strings
    """
    self._complete_maintenance(Hosts(set(hostnames)))

  def start_maintenance(self, hostnames):
    """Put a list of hostnames into maintenance mode, to de-prioritize scheduling.

    This is part of two-phase draining- tasks will still be running on these hosts until
    drain_hosts is called upon them.

    :param hostnames: List of hosts to set for initial maintenance
    :type hostnames: list of strings
    :rtype: list of hostnames with the maintenance mode set
    """
    resp = self._client.start_maintenance(Hosts(set(hostnames)))
    check_and_log_response(resp)
    result = [host_status.host for host_status in resp.result.startMaintenanceResult.statuses]
    if len(result) != len(hostnames):
      log.warning('Skipping maintenance for unknown hosts: %s' % (set(hostnames) - set(result)))

    return result

  def _operate_on_hosts(self, drained_hosts, callback):
    """Perform a given operation on a list of hosts that are ready for maintenance.

    :param drained_hosts: Hosts that have been drained (via _drain_hosts)
    :type drained_hosts: list of strings
    :param callback: Function to call one hostname at a time
    :type callback: function
    """
    for hostname in drained_hosts:
      callback(hostname)

  def perform_maintenance(self, hostnames, grouping_function=DEFAULT_GROUPING,
                          percentage=None, duration=None, output_file=None, callback=None):
    """Put hosts into maintenance mode and drain them.

    Walk through the process of putting hosts into maintenance and draining them of tasks. The hosts
    will remain in maintenance mode upon completion.


    :param hostnames: A list of hostnames to operate upon
    :type hostnames: list of strings
    :param grouping_function: How to split up the hostname into groups
    :type grouping_function: function
    :param percentage: SLA percentage to use
    :type percentage: float
    :param duration: SLA duration to use
    :type duration: twitter.common.quantity.Time
    :param output_file: file to write hosts that were not drained due to failed SLA check
    :type output_file: string
    :param callback: Function to call once hosts are drained
    :type callback: function
    :rtype: set of host names that were successfully drained
    """
    hostnames = self.start_maintenance(hostnames)
    not_drained_hostnames = set()

    for hosts in self.iter_batches(hostnames, grouping_function):
      log.info('Beginning SLA check for %s' % hosts.hostNames)
      unsafe_hostnames = self._check_sla(
          list(hosts.hostNames),
          grouping_function,
          percentage,
          duration)

      if unsafe_hostnames:
        log.warning('Some hosts did not pass SLA check and will not be drained! '
                    'Skipping hosts: %s' % unsafe_hostnames)
        not_drained_hostnames |= unsafe_hostnames
        drainable_hostnames = hosts.hostNames - unsafe_hostnames
        if not drainable_hostnames:
          continue
        hosts = Hosts(drainable_hostnames)
      else:
        log.info('All hosts passed SLA check.')

      not_drained_hostnames |= self._drain_hosts(hosts)

      if callback:
        self._operate_on_hosts(hosts.hostNames - not_drained_hostnames, callback)

    if not_drained_hostnames:
      output = '\n'.join(list(not_drained_hostnames))
      log.info('The following hosts WERE NOT DRAINED due to failed SLA check or external failures:')
      print(output)
      if output_file:
        try:
          with open(output_file, 'w') as fp:
            fp.write(output)
            fp.write('\n')
          log.info('Written unsafe host names into: %s' % output_file)
        except IOError as e:
          log.error('Failed to write into the output file: %s' % e)

    return set(hostnames) - not_drained_hostnames

  def check_status(self, hostnames):
    """Query the scheduler to determine the maintenance status for a list of hostnames

    :param hostnames: Hosts to query for
    :type hostnames: list of strings
    :rtype: list of 2-tuples, hostname and MaintenanceMode
    """
    resp = self._client.maintenance_status(Hosts(set(hostnames)))
    check_and_log_response(resp)
    statuses = []
    for host_status in resp.result.maintenanceStatusResult.statuses:
      statuses.append((host_status.host, MaintenanceMode._VALUES_TO_NAMES[host_status.mode]))
    return statuses
