import unittest

from twitter.aurora.admin.mesos_maintenance import MesosMaintenance
from twitter.aurora.common_internal.clusters import TWITTER_CLUSTERS

from gen.twitter.aurora.ttypes import Response, ResponseCode

import mock

MOCK_TEST_HOSTS = ['smf1-jim-27-sr3.prod.twitter.com']


class TestMesosMaintenance(unittest.TestCase):
  @mock.patch("twitter.aurora.client.api.AuroraClientAPI.start_maintenance")
  def test_start_maintenance(self, mock_api):
    mock_api.return_value = Response(responseCode=ResponseCode.OK)
    maintenance = MesosMaintenance(TWITTER_CLUSTERS["local"], "non-verbose")
    maintenance.start_maintenance(MOCK_TEST_HOSTS)
