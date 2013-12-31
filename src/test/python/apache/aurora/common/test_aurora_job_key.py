import unittest

from apache.aurora.common.aurora_job_key import AuroraJobKey

# TODO(ksweeney): Moar coverage
class AuroraJobKeyTest(unittest.TestCase):
  def test_basic(self):
    AuroraJobKey.from_path("smf1/mesos/test/labrat")
