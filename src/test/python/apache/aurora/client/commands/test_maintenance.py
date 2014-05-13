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

import contextlib

from mock import Mock, patch

from apache.aurora.client.commands.maintenance import (
    end_maintenance_hosts,
    host_maintenance_status,
    perform_maintenance_hosts,
    start_maintenance_hosts
)
from apache.aurora.client.commands.util import AuroraClientCommandTest

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
    return mock_options

  def create_host_statuses(self, maintenance_mode):
    return [HostStatus(host=hostname, mode=maintenance_mode) for hostname in self.HOSTNAMES]

  def create_start_maintenance_result(self):
    host_statuses = self.create_host_statuses(MaintenanceMode.SCHEDULED)
    response = self.create_simple_success_response()
    response.result.maintenanceStatusResult = StartMaintenanceResult(statuses=set(host_statuses))
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
        patch('apache.aurora.client.commands.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters_maintenancepatch,
            options):
      start_maintenance_hosts([self.TEST_CLUSTER])

      mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))

  def test_end_maintenance_hosts(self):
    mock_options = self.make_mock_options()
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.endMaintenance.return_value = self.create_end_maintenance_result()
    mock_scheduler_proxy.maintenanceStatus.return_value = self.create_maintenance_status_result()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.commands.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters_maintenancepatch,
            options):
      end_maintenance_hosts([self.TEST_CLUSTER])

      mock_scheduler_proxy.endMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))
      mock_scheduler_proxy.maintenanceStatus.assert_called_with(Hosts(set(self.HOSTNAMES)))

  def test_perform_maintenance_hosts(self):
    mock_options = self.make_mock_options()
    mock_options.post_drain_script = None
    mock_options.groups_per_batch = '1'
    mock_options.grouping = 'by_host'

    def host_status_results(hostnames):
      if isinstance(hostnames, Hosts):
        return self.create_drained_status_result(hostnames)
      return self.create_maintenance_status_result()

    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.endMaintenance.return_value = self.create_end_maintenance_result()
    mock_scheduler_proxy.maintenanceStatus.side_effect = host_status_results
    mock_scheduler_proxy.startMaintenance.return_value = self.create_start_maintenance_result()
    mock_scheduler_proxy.drainHosts.return_value = self.create_start_maintenance_result()

    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.commands.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_sleep,
            mock_scheduler_proxy_class,
            mock_clusters_maintenancepatch,
            options):
      perform_maintenance_hosts([self.TEST_CLUSTER])

      mock_scheduler_proxy.startMaintenance.assert_called_with(Hosts(set(self.HOSTNAMES)))
      #TODO(jsmith): Consider not mocking out sleep and instead refactoring
      assert mock_sleep.call_count == 3
      assert mock_scheduler_proxy.maintenanceStatus.call_count == 7
      assert mock_scheduler_proxy.drainHosts.call_count == 3
      assert mock_scheduler_proxy.endMaintenance.call_count == 4

  def test_host_maintenance_status(self):
    mock_options = self.make_mock_options()
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.maintenanceStatus.return_value = self.create_maintenance_status_result()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.commands.maintenance.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters_maintenancepatch,
            options):
      host_maintenance_status([self.TEST_CLUSTER])

      mock_scheduler_proxy.maintenanceStatus.assert_called_with(Hosts(set(self.HOSTNAMES)))
