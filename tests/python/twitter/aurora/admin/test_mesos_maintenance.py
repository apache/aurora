from twitter.aurora.admin.mesos_maintenance import MesosMaintenance
from twitter.aurora.common_internal.clusters import TWITTER_CLUSTERS

from mox import MoxTestBase


class TestMesosMaintenance(MoxTestBase):
  def setUp(self):
    self.maintenance = MesosMaintenance(TWITTER_CLUSTERS["local"], "non-verbose")
