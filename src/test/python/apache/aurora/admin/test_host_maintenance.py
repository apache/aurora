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

import copy
import time
import unittest

import mock
from twitter.common import log
from twitter.common.quantity import Time

from apache.aurora.admin.host_maintenance import HostMaintenance
from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.base import add_grouping, remove_grouping
from apache.aurora.common.cluster import Cluster

from gen.apache.aurora.api.ttypes import (
    Hosts,
    HostStatus,
    MaintenanceMode,
    MaintenanceStatusResult,
    Response,
    ResponseCode,
    Result
)

DEFAULT_CLUSTER = Cluster(
    name='us-west',
    scheduler_uri='us-west-234.example.com:8888',
)
TEST_HOSTNAMES = [
    'us-west-001.example.com',
    'us-west-002.example.com',
    'us-west-003.example.com']


class TestHostMaintenance(unittest.TestCase):
  @mock.patch("apache.aurora.client.api.AuroraClientAPI.maintenance_status",
      spec=AuroraClientAPI.maintenance_status)
  @mock.patch("apache.aurora.client.api.AuroraClientAPI.drain_hosts",
      spec=AuroraClientAPI.drain_hosts)
  def test_drain_hosts(self, mock_drain_hosts, mock_maintenance_status):
    fake_maintenance_status_response = [
        Response(result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
            HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.SCHEDULED),
            HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.SCHEDULED),
            HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.SCHEDULED)
        ])))),
        Response(result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
            HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.DRAINING),
            HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.DRAINING),
            HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.DRAINING)
        ])))),
        Response(result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
            HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.DRAINING),
            HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.DRAINED),
            HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.DRAINED)
        ])))),
        Response(result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
            HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.DRAINED)
        ]))))
    ]
    fake_maintenance_status_call_args = []
    def fake_maintenance_status_side_effect(hosts):
      fake_maintenance_status_call_args.append(copy.deepcopy(hosts))
      return fake_maintenance_status_response.pop(0)

    clock = mock.Mock(time)
    mock_drain_hosts.return_value = Response(responseCode=ResponseCode.OK)
    mock_maintenance_status.side_effect = fake_maintenance_status_side_effect
    test_hosts = Hosts(set(TEST_HOSTNAMES))
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance._drain_hosts(test_hosts, clock)
    mock_drain_hosts.assert_called_once_with(test_hosts)
    assert clock.sleep.call_count == 4
    assert clock.sleep.call_args == mock.call(
        HostMaintenance.START_MAINTENANCE_DELAY.as_(Time.SECONDS))
    assert mock_maintenance_status.call_count == 4
    assert fake_maintenance_status_call_args == [
        (Hosts(set(TEST_HOSTNAMES))),
        (Hosts(set(TEST_HOSTNAMES))),
        (Hosts(set(TEST_HOSTNAMES))),
        (Hosts(set([TEST_HOSTNAMES[0]])))]

  @mock.patch("twitter.common.log.warning", spec=log.warning)
  @mock.patch("apache.aurora.client.api.AuroraClientAPI.maintenance_status",
      spec=AuroraClientAPI.maintenance_status)
  @mock.patch("apache.aurora.client.api.AuroraClientAPI.end_maintenance",
      spec=AuroraClientAPI.end_maintenance)
  def test_complete_maintenance(self, mock_end_maintenance, mock_maintenance_status, mock_warning):
    mock_maintenance_status.return_value = Response(result=Result(
        maintenanceStatusResult=MaintenanceStatusResult(set([
            HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.NONE),
            HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.NONE),
            HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.DRAINED)
        ]))
    ))
    mock_end_maintenance.return_value = Response(responseCode=ResponseCode.OK)
    test_hosts = Hosts(set(TEST_HOSTNAMES))
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance._complete_maintenance(test_hosts)
    mock_end_maintenance.assert_called_once_with(test_hosts)
    mock_maintenance_status.assert_called_once_with(test_hosts)
    mock_warning.assert_called_once_with('%s is DRAINING or in DRAINED' % TEST_HOSTNAMES[2])

  def test_operate_on_hosts(self):
    mock_callback = mock.Mock()
    test_hosts = Hosts(TEST_HOSTNAMES)
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance._operate_on_hosts(test_hosts, mock_callback)
    assert mock_callback.call_count == 3

  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._complete_maintenance",
    spec=HostMaintenance._complete_maintenance)
  def test_end_maintenance(self, mock_complete_maintenance):
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.end_maintenance(TEST_HOSTNAMES)
    mock_complete_maintenance.assert_called_once_with(Hosts(set(TEST_HOSTNAMES)))

  @mock.patch("apache.aurora.client.api.AuroraClientAPI.start_maintenance",
      spec=AuroraClientAPI.start_maintenance)
  def test_start_maintenance(self, mock_api):
    mock_api.return_value = Response(responseCode=ResponseCode.OK)
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.start_maintenance(TEST_HOSTNAMES)
    mock_api.assert_called_once_with(Hosts(set(TEST_HOSTNAMES)))

  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._complete_maintenance",
    spec=HostMaintenance._complete_maintenance)
  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._operate_on_hosts",
    spec=HostMaintenance._operate_on_hosts)
  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._drain_hosts",
    spec=HostMaintenance._drain_hosts)
  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance.start_maintenance",
    spec=HostMaintenance.start_maintenance)
  def test_perform_maintenance(self, mock_start_maintenance, mock_drain_hosts,
      mock_operate_on_hosts, mock_complete_maintenance):
    mock_callback = mock.Mock()
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.perform_maintenance(TEST_HOSTNAMES, callback=mock_callback)
    mock_start_maintenance.assert_called_once_with(TEST_HOSTNAMES)
    assert mock_drain_hosts.call_count == 3
    assert mock_drain_hosts.call_args_list == [
        mock.call(Hosts(set([hostname]))) for hostname in TEST_HOSTNAMES]
    assert mock_operate_on_hosts.call_count == 3
    assert mock_operate_on_hosts.call_args_list == [
        mock.call(Hosts(set([hostname])), mock_callback) for hostname in TEST_HOSTNAMES]
    assert mock_complete_maintenance.call_count == 3
    assert mock_complete_maintenance.call_args_list == [
        mock.call(Hosts(set([hostname]))) for hostname in TEST_HOSTNAMES]

  @mock.patch("apache.aurora.client.api.AuroraClientAPI.maintenance_status",
      spec=AuroraClientAPI.maintenance_status)
  def test_check_status(self, mock_maintenance_status):
    mock_maintenance_status.return_value = Response(responseCode=ResponseCode.OK, result=Result(
        maintenanceStatusResult=MaintenanceStatusResult(set([
            HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.DRAINING),
            HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.DRAINED),
            HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.NONE)
        ]))
    ))
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    statuses = maintenance.check_status(TEST_HOSTNAMES)
    mock_maintenance_status.assert_called_once_with(Hosts(set(TEST_HOSTNAMES)))
    assert statuses == [
        (TEST_HOSTNAMES[0], MaintenanceMode._VALUES_TO_NAMES[MaintenanceMode.DRAINING]),
        (TEST_HOSTNAMES[1], MaintenanceMode._VALUES_TO_NAMES[MaintenanceMode.DRAINED]),
        (TEST_HOSTNAMES[2], MaintenanceMode._VALUES_TO_NAMES[MaintenanceMode.NONE])
    ]


def test_default_grouping():
  example_host_list = [
    'xyz321.example.com',
    'bar337.example.com',
    'foo001.example.com',
  ]

  batches = list(HostMaintenance.iter_batches(example_host_list))
  assert batches[0] == Hosts(set(['bar337.example.com']))
  assert batches[1] == Hosts(set(['foo001.example.com']))
  assert batches[2] == Hosts(set(['xyz321.example.com']))


def rack_grouping(hostname):
  return hostname.split('-')[1]


def test_rack_grouping():
  add_grouping('by_rack', rack_grouping)

  example_host_list = [
    'west-aaa-001.example.com',
    'west-aaa-002.example.com',
    'west-xyz-002.example.com',
    'east-xyz-003.example.com',
    'east-xyz-004.example.com',
  ]

  try:
    batches = list(HostMaintenance.iter_batches(example_host_list, 'by_rack'))
    assert batches[0] == Hosts(set([
        'west-aaa-001.example.com',
        'west-aaa-002.example.com'
    ]))
    assert batches[1] == Hosts(set([
        'west-xyz-002.example.com',
        'east-xyz-003.example.com',
        'east-xyz-004.example.com',
    ]))

  finally:
    remove_grouping('by_rack')
