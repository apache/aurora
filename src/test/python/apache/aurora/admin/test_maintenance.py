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

import contextlib

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.admin.host_maintenance import HostMaintenance
from apache.aurora.admin.maintenance import host_activate, host_deactivate, host_drain, host_status

from .util import AuroraClientCommandTest

from gen.apache.aurora.api.ttypes import (
    DrainHostsResult,
    EndMaintenanceResult,
    Hosts,
    HostStatus,
    MaintenanceMode,
    MaintenanceStatusResult,
    StartMaintenanceResult
)


class TestMaintenanceCommands(AuroraClientCommandTest):
  HOSTNAMES = ['us-grf-20', 'us-jim-47', 'us-suz-01']

  def make_mock_options(self):
    mock_options = Mock()
    mock_options.filename = None
    mock_options.hosts = ','.join(self.HOSTNAMES)
    mock_options.cluster = self.TEST_CLUSTER
    mock_options.verbosity = False
    mock_options.disable_all_hooks = False
    mock_options.percentage = None
    mock_options.duration = None
    mock_options.reason = None
    return mock_options

  def create_host_statuses(self, maintenance_mode, skip_hosts=None):
    return [HostStatus(host=hostname, mode=maintenance_mode) for hostname in self.HOSTNAMES
            if not skip_hosts or hostname not in skip_hosts]

  def create_start_maintenance_result(self, skip_hosts=None):
    host_statuses = self.create_host_statuses(MaintenanceMode.SCHEDULED, skip_hosts)
    response = self.create_simple_success_response()
    response.result.startMaintenanceResult = StartMaintenanceResult(statuses=set(host_statuses))
    return response

  def create_end_maintenance_result(self):
    host_statuses = self.create_host_statuses(MaintenanceMode.NONE)
    response = self.create_simple_success_response()
    response.result.endMaintenanceResult = EndMaintenanceResult(statuses=set(host_statuses))
    return response

  def create_drain_hosts_result(self):
    host_statuses = self.create_host_statuses(MaintenanceMode.DRAINING)
    response = self.create_simple_success_response()
    response.result.drainHostsResult = DrainHostsResult(statuses=set(host_statuses))
    return response

  def create_maintenance_status_result(self):
    host_statuses = self.create_host_statuses(MaintenanceMode.NONE)
    response = self.create_simple_success_response()
    response.result.maintenanceStatusResult = MaintenanceStatusResult(statuses=set(host_statuses))
    return response

  def create_drained_status_result(self, hosts):
    host_statuses = [
        HostStatus(host=hostname, mode=MaintenanceMode.DRAINED) for hostname in hosts.hostNames]
    response = self.create_simple_success_response()
    response.result.maintenanceStatusResult = MaintenanceStatusResult(statuses=set(host_statuses))
    return response

  def test_start_maintenance_hosts(self):
    mock_options = self.make_mock_options()
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.startMaintenance.return_value = self.create_start_maintenance_result()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)):
      host_deactivate([self.TEST_CLUSTER])

      mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))

  def test_end_maintenance_hosts(self):
    mock_options = self.make_mock_options()
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.endMaintenance.return_value = self.create_end_maintenance_result()
    mock_scheduler_proxy.maintenanceStatus.return_value = self.create_maintenance_status_result()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)):
      host_activate([self.TEST_CLUSTER])

      mock_scheduler_proxy.endMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))
      mock_scheduler_proxy.maintenanceStatus.assert_called_with(Hosts(set(self.HOSTNAMES)))

  def test_perform_maintenance_hosts(self):
    mock_options = self.make_mock_options()
    mock_options.post_drain_script = 'callback'
    mock_options.grouping = 'by_host'

    def host_status_results(hostnames):
      if isinstance(hostnames, Hosts):
        return self.create_drained_status_result(hostnames)
      return self.create_maintenance_status_result()

    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_callback = Mock()
    mock_scheduler_proxy.maintenanceStatus.side_effect = host_status_results
    mock_scheduler_proxy.startMaintenance.return_value = self.create_start_maintenance_result()
    mock_scheduler_proxy.drainHosts.return_value = self.create_start_maintenance_result()
    mock_vector = self.create_mock_probe_hosts_vector([
        self.create_probe_hosts(self.HOSTNAMES[0], 95, True, None),
        self.create_probe_hosts(self.HOSTNAMES[1], 95, True, None),
        self.create_probe_hosts(self.HOSTNAMES[2], 95, True, None)
    ])

    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.sla.Sla.get_domain_uptime_vector',
              return_value=mock_vector),
        patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.admin.maintenance.parse_script', return_value=mock_callback),
        patch('threading._Event.wait'),
        patch('twitter.common.app.get_options', return_value=mock_options)):
      host_drain([self.TEST_CLUSTER])

      mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))
      assert mock_scheduler_proxy.maintenanceStatus.call_count == 3
      assert mock_scheduler_proxy.drainHosts.call_count == 3
      assert mock_callback.call_count == 3

  def test_perform_maintenance_hosts_unknown_hosts_skipped(self):
    mock_options = self.make_mock_options()
    mock_options.post_drain_script = None
    mock_options.grouping = 'by_host'

    def host_status_results(hostnames):
      if isinstance(hostnames, Hosts):
        return self.create_drained_status_result(hostnames)
      return self.create_maintenance_status_result()

    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.maintenanceStatus.side_effect = host_status_results
    mock_scheduler_proxy.startMaintenance.return_value = self.create_start_maintenance_result(
        skip_hosts=['us-grf-20'])
    mock_scheduler_proxy.drainHosts.return_value = self.create_start_maintenance_result()
    mock_vector = self.create_mock_probe_hosts_vector([
      self.create_probe_hosts(self.HOSTNAMES[0], 95, True, None),
      self.create_probe_hosts(self.HOSTNAMES[1], 95, True, None),
      self.create_probe_hosts(self.HOSTNAMES[2], 95, True, None)
    ])

    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.sla.Sla.get_domain_uptime_vector',
              return_value=mock_vector),
        patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('threading._Event.wait'),
        patch('twitter.common.app.get_options', return_value=mock_options)):
      host_drain([self.TEST_CLUSTER])

      mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))
      assert mock_scheduler_proxy.maintenanceStatus.call_count == 2
      assert mock_scheduler_proxy.drainHosts.call_count == 2

  def test_perform_maintenance_hosts_failed_default_sla(self):
    with temporary_file() as fp:
      mock_options = self.make_mock_options()
      mock_options.post_drain_script = None
      mock_options.grouping = 'by_host'
      mock_options.unsafe_hosts_filename = fp.name

      mock_api, mock_scheduler_proxy = self.create_mock_api()
      mock_scheduler_proxy.startMaintenance.return_value = self.create_start_maintenance_result()
      mock_scheduler_proxy.drainHosts.return_value = self.create_start_maintenance_result()
      mock_vector = self.create_mock_probe_hosts_vector([
          self.create_probe_hosts(self.HOSTNAMES[0], 95, False, None),
          self.create_probe_hosts(self.HOSTNAMES[1], 95, False, None),
          self.create_probe_hosts(self.HOSTNAMES[2], 95, False, None)
      ])

      with contextlib.nested(
          patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
          patch('apache.aurora.client.api.sla.Sla.get_domain_uptime_vector',
                return_value=mock_vector),
          patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('threading._Event.wait'),
          patch('twitter.common.app.get_options', return_value=mock_options)):
        host_drain([self.TEST_CLUSTER])

        mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))

  def test_perform_maintenance_hosts_failed_custom_sla(self):
    with temporary_file() as fp:
      mock_options = self.make_mock_options()
      mock_options.post_drain_script = None
      mock_options.grouping = 'by_host'
      mock_options.percentage = 50
      mock_options.duration = '10m'
      mock_options.reason = 'Test overrides'
      mock_options.unsafe_hosts_filename = fp.name

      mock_api, mock_scheduler_proxy = self.create_mock_api()
      mock_scheduler_proxy.startMaintenance.return_value = self.create_start_maintenance_result()
      mock_scheduler_proxy.drainHosts.return_value = self.create_start_maintenance_result()
      mock_vector = self.create_mock_probe_hosts_vector([
          self.create_probe_hosts(self.HOSTNAMES[0], 95, False, None),
          self.create_probe_hosts(self.HOSTNAMES[1], 95, False, None),
          self.create_probe_hosts(self.HOSTNAMES[2], 95, False, None)
      ])
      mock_wait = Mock()

      with contextlib.nested(
          patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
          patch('apache.aurora.client.api.sla.Sla.get_domain_uptime_vector',
                return_value=mock_vector),
          patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('apache.aurora.admin.admin_util.log_admin_message'),
          patch('threading._Event.wait', return_value=mock_wait),
          patch('twitter.common.app.get_options', return_value=mock_options)
      ) as (_, _, _, log, _, _):

        host_drain([self.TEST_CLUSTER])

        assert 'Test overrides' in log.call_args[0][1]
        mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))
        mock_wait.called_once_with(HostMaintenance.MAX_STATUS_WAIT)

  def test_perform_maintenance_hosts_no_prod_tasks(self):
    mock_options = self.make_mock_options()
    mock_options.post_drain_script = None
    mock_options.grouping = 'by_host'

    def host_status_results(hostnames):
      if isinstance(hostnames, Hosts):
        return self.create_drained_status_result(hostnames)
      return self.create_maintenance_status_result()

    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.maintenanceStatus.side_effect = host_status_results
    mock_scheduler_proxy.startMaintenance.return_value = self.create_start_maintenance_result()
    mock_scheduler_proxy.drainHosts.return_value = self.create_start_maintenance_result()

    def create_empty_sla_results():
      mock_vector = Mock()
      mock_vector.probe_hosts.return_value = []
      return mock_vector

    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.sla.Sla.get_domain_uptime_vector',
              return_value=create_empty_sla_results()),
        patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('threading._Event.wait'),
        patch('twitter.common.app.get_options', return_value=mock_options)):

      host_drain([self.TEST_CLUSTER])

      mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))
      assert mock_scheduler_proxy.maintenanceStatus.call_count == 3
      assert mock_scheduler_proxy.drainHosts.call_count == 3

  def test_perform_maintenance_hosts_multiple_sla_groups_failure(self):
    mock_options = self.make_mock_options()
    mock_options.post_drain_script = None
    mock_options.grouping = 'by_host'
    mock_options.unsafe_hosts_filename = None

    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.startMaintenance.return_value = self.create_start_maintenance_result()

    def create_multiple_sla_results():
      mock_vector = Mock()
      mock_vector.probe_hosts.return_value = self.HOSTNAMES
      return mock_vector

    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.api.sla.Sla.get_domain_uptime_vector',
              return_value=create_multiple_sla_results()),
        patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)):

      host_drain([self.TEST_CLUSTER])

      mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))

  def test_perform_maintenance_hosts_reason_missing(self):
    mock_options = self.make_mock_options()
    mock_options.grouping = 'by_host'
    mock_options.percentage = 50
    mock_options.duration = '10m'

    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options)):
      self.assertRaises(SystemExit, host_drain, [self.TEST_CLUSTER])

  def test_host_maintenance_status(self):
    mock_options = self.make_mock_options()
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.maintenanceStatus.return_value = self.create_maintenance_status_result()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.admin.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)):
      host_status([self.TEST_CLUSTER])

      mock_scheduler_proxy.maintenanceStatus.assert_called_with(Hosts(set(self.HOSTNAMES)))
