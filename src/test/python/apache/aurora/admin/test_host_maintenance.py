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

import unittest

import mock
import pytest

from apache.aurora.admin.host_maintenance import HostMaintenance
from apache.aurora.common.cluster import Cluster

from gen.apache.aurora.api.ttypes import Hosts, Response, ResponseCode

DEFAULT_CLUSTER = Cluster(
    name='us-west',
    scheduler_uri='us-west-234.example.com:8888',
)
MOCK_TEST_HOSTS = ['us-west-001.example.com']


class TestHostMaintenance(unittest.TestCase):
  @mock.patch("apache.aurora.client.api.AuroraClientAPI.start_maintenance")
  def test_start_maintenance(self, mock_api):
    mock_api.return_value = Response(responseCode=ResponseCode.OK)
    maintenance = HostMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.start_maintenance(MOCK_TEST_HOSTS)


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
  old_grouping_functions = HostMaintenance.GROUPING_FUNCTIONS.copy()
  HostMaintenance.GROUPING_FUNCTIONS['by_rack'] = rack_grouping

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
    HostMaintenance.GROUPING_FUNCTIONS = old_grouping_functions
