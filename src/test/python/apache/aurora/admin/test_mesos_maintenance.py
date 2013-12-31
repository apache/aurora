import unittest

from apache.aurora.admin.mesos_maintenance import MesosMaintenance
from apache.aurora.common.cluster import Cluster

from gen.apache.aurora.ttypes import (
    Hosts,
    Response,
    ResponseCode,
)

import mock
import pytest


DEFAULT_CLUSTER = Cluster(
    name = 'us-west',
    scheduler_uri = 'us-west-234.example.com:8888',
)
MOCK_TEST_HOSTS = ['us-west-001.example.com']


class TestMesosMaintenance(unittest.TestCase):
  @mock.patch("apache.aurora.client.api.AuroraClientAPI.start_maintenance")
  def test_start_maintenance(self, mock_api):
    mock_api.return_value = Response(responseCode=ResponseCode.OK)
    maintenance = MesosMaintenance(DEFAULT_CLUSTER, 'quiet')
    maintenance.start_maintenance(MOCK_TEST_HOSTS)


def test_default_grouping():
  example_host_list = [
    'xyz321.example.com',
    'bar337.example.com',
    'foo001.example.com',
  ]

  batches = list(MesosMaintenance.iter_batches(example_host_list, 1))
  assert batches[0] == Hosts(set(['bar337.example.com']))
  assert batches[1] == Hosts(set(['foo001.example.com']))
  assert batches[2] == Hosts(set(['xyz321.example.com']))

  batches = list(MesosMaintenance.iter_batches(example_host_list, 2))
  assert batches[0] == Hosts(set(['bar337.example.com', 'foo001.example.com']))
  assert batches[1] == Hosts(set(['xyz321.example.com']))



def rack_grouping(hostname):
  return hostname.split('-')[1]


def test_rack_grouping():
  old_grouping_functions = MesosMaintenance.GROUPING_FUNCTIONS.copy()
  MesosMaintenance.GROUPING_FUNCTIONS['by_rack'] = rack_grouping

  example_host_list = [
    'west-aaa-001.example.com',
    'west-aaa-002.example.com',
    'west-xyz-002.example.com',
    'east-xyz-003.example.com',
    'east-xyz-004.example.com',
  ]

  try:
    batches = list(MesosMaintenance.iter_batches(example_host_list, 1, 'by_rack'))
    assert batches[0] == Hosts(set([
        'west-aaa-001.example.com',
        'west-aaa-002.example.com'
    ]))
    assert batches[1] == Hosts(set([
        'west-xyz-002.example.com',
        'east-xyz-003.example.com',
        'east-xyz-004.example.com',
    ]))

    batches = list(MesosMaintenance.iter_batches(example_host_list, 2, 'by_rack'))
    assert batches[0] == Hosts(set(example_host_list))

    batches = list(MesosMaintenance.iter_batches(example_host_list, 3, 'by_rack'))
    assert batches[0] == Hosts(set(example_host_list))

    with pytest.raises(ValueError):
      list(MesosMaintenance.iter_batches(example_host_list, 0))

  finally:
    MesosMaintenance.GROUPING_FUNCTIONS = old_grouping_functions
