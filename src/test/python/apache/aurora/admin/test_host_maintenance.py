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
import unittest
from contextlib import contextmanager

import mock
from twitter.common import log
from twitter.common.contextutil import temporary_file
from twitter.common.quantity import Amount, Time

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
    Result,
    StartMaintenanceResult
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
  @mock.patch("threading._Event.wait")
  def test_drain_hosts(self, mock_event_wait, mock_drain_hosts, mock_maintenance_status):
    fake_maintenance_status_response = [
        Response(
            responseCode=ResponseCode.OK,
            result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
                HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.SCHEDULED),
                HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.SCHEDULED),
                HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.SCHEDULED)
            ])))),
        Response(
            responseCode=ResponseCode.OK,
            result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
                HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.DRAINING),
                HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.DRAINING),
                HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.DRAINING)
            ])))),
        Response(
            responseCode=ResponseCode.OK,
            result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
                HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.DRAINING),
                HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.DRAINED),
                HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.DRAINED)
            ])))),
        Response(
            responseCode=ResponseCode.OK,
            result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
                HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.DRAINED),
                HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.DRAINED),
                HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.DRAINED)
            ]))))]

    fake_maintenance_status_call_args = []
    def fake_maintenance_status_side_effect(hosts):
      fake_maintenance_status_call_args.append(copy.deepcopy(hosts))
      return fake_maintenance_status_response.pop(0)

    mock_drain_hosts.return_value = Response(responseCode=ResponseCode.OK)
    mock_maintenance_status.side_effect = fake_maintenance_status_side_effect
    test_hosts = Hosts(set(TEST_HOSTNAMES))
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')

    not_drained_hostnames = maintenance._drain_hosts(test_hosts)
    assert len(not_drained_hostnames) == 0
    mock_drain_hosts.assert_called_once_with(test_hosts)
    assert mock_maintenance_status.call_count == 4
    assert mock_event_wait.call_count == 4
    assert fake_maintenance_status_call_args == [
        (Hosts(set(TEST_HOSTNAMES))),
        (Hosts(set(TEST_HOSTNAMES))),
        (Hosts(set(TEST_HOSTNAMES))),
        (Hosts(set(TEST_HOSTNAMES)))]

  @mock.patch("apache.aurora.client.api.AuroraClientAPI.maintenance_status",
              spec=AuroraClientAPI.maintenance_status)
  @mock.patch("apache.aurora.client.api.AuroraClientAPI.drain_hosts",
              spec=AuroraClientAPI.drain_hosts)
  @mock.patch("threading._Event.wait")
  def test_drain_hosts_timed_out_wait(self, _, mock_drain_hosts, mock_maintenance_status):
    fake_maintenance_status_response = Response(
        responseCode=ResponseCode.OK,
        result=Result(maintenanceStatusResult=MaintenanceStatusResult(set([
          HostStatus(host=TEST_HOSTNAMES[0], mode=MaintenanceMode.SCHEDULED),
          HostStatus(host=TEST_HOSTNAMES[1], mode=MaintenanceMode.SCHEDULED),
          HostStatus(host=TEST_HOSTNAMES[2], mode=MaintenanceMode.SCHEDULED)
        ]))))

    mock_drain_hosts.return_value = Response(responseCode=ResponseCode.OK)
    mock_maintenance_status.return_value = fake_maintenance_status_response
    test_hosts = Hosts(set(TEST_HOSTNAMES))
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.MAX_STATUS_WAIT = Amount(1, Time.MILLISECONDS)

    not_drained_hostnames = maintenance._drain_hosts(test_hosts)
    assert TEST_HOSTNAMES == sorted(not_drained_hostnames)
    assert mock_maintenance_status.call_count == 1
    mock_drain_hosts.assert_called_once_with(test_hosts)
    mock_maintenance_status.assert_called_once_with((Hosts(set(TEST_HOSTNAMES))))

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

  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._complete_maintenance",
    spec=HostMaintenance._complete_maintenance)
  def test_end_maintenance(self, mock_complete_maintenance):
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.end_maintenance(TEST_HOSTNAMES)
    mock_complete_maintenance.assert_called_once_with(Hosts(set(TEST_HOSTNAMES)))

  @mock.patch("apache.aurora.client.api.AuroraClientAPI.start_maintenance",
      spec=AuroraClientAPI.start_maintenance)
  def test_start_maintenance(self, mock_api):
    mock_api.return_value = Response(responseCode=ResponseCode.OK,
        result=Result(startMaintenanceResult=StartMaintenanceResult(statuses=set([HostStatus()]))))
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.start_maintenance(TEST_HOSTNAMES)
    mock_api.assert_called_once_with(Hosts(set(TEST_HOSTNAMES)))

  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._drain_hosts",
    spec=HostMaintenance._drain_hosts)
  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance.start_maintenance",
    spec=HostMaintenance.start_maintenance)
  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._check_sla",
    spec=HostMaintenance._check_sla)
  def test_perform_maintenance(self, mock_check_sla, mock_start_maintenance, mock_drain_hosts):
    mock_check_sla.return_value = set()
    mock_start_maintenance.return_value = TEST_HOSTNAMES
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.perform_maintenance(TEST_HOSTNAMES)
    mock_start_maintenance.assert_called_once_with(TEST_HOSTNAMES)
    assert mock_check_sla.call_count == 3
    assert mock_drain_hosts.call_count == 3
    assert mock_drain_hosts.call_args_list == [
        mock.call(Hosts(set([hostname]))) for hostname in TEST_HOSTNAMES]

  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._drain_hosts",
              spec=HostMaintenance._drain_hosts)
  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance.start_maintenance",
              spec=HostMaintenance.start_maintenance)
  @mock.patch("apache.aurora.admin.host_maintenance.HostMaintenance._check_sla",
              spec=HostMaintenance._check_sla)
  def test_perform_maintenance_partial_sla_failure(self, mock_check_sla, mock_start_maintenance,
                               mock_drain_hosts):
    failed_host = 'us-west-001.example.com'
    mock_check_sla.return_value = set([failed_host])
    mock_start_maintenance.return_value = TEST_HOSTNAMES
    drained_hosts = set(TEST_HOSTNAMES) - set([failed_host])
    mock_drain_hosts.return_value = set()
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')

    with temporary_file() as fp:
      with group_by_rack():
        drained = maintenance.perform_maintenance(
            TEST_HOSTNAMES,
            grouping_function='by_rack',
            output_file=fp.name)

        with open(fp.name, 'r') as fpr:
          content = fpr.read()
          assert failed_host in content

        mock_start_maintenance.assert_called_once_with(TEST_HOSTNAMES)
        assert len(drained) == 2
        assert failed_host not in drained
        assert mock_check_sla.call_count == 1
        assert mock_drain_hosts.call_count == 1
        assert mock_drain_hosts.call_args_list == [mock.call(Hosts(drained_hosts))]

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
    result = maintenance.check_status(TEST_HOSTNAMES)
    mock_maintenance_status.assert_called_once_with(Hosts(set(TEST_HOSTNAMES)))

    assert len(result) == 3
    assert (TEST_HOSTNAMES[0], MaintenanceMode._VALUES_TO_NAMES[MaintenanceMode.DRAINING]) in result
    assert (TEST_HOSTNAMES[1], MaintenanceMode._VALUES_TO_NAMES[MaintenanceMode.DRAINED]) in result
    assert (TEST_HOSTNAMES[2], MaintenanceMode._VALUES_TO_NAMES[MaintenanceMode.NONE]) in result


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


@contextmanager
def group_by_rack():
  add_grouping('by_rack', rack_grouping)
  yield
  remove_grouping('by_rack')


def rack_grouping(hostname):
  return hostname.split('-')[1]


def test_rack_grouping():
  example_host_list = [
    'west-aaa-001.example.com',
    'west-aaa-002.example.com',
    'west-xyz-002.example.com',
    'east-xyz-003.example.com',
    'east-xyz-004.example.com',
  ]

  with group_by_rack():
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
