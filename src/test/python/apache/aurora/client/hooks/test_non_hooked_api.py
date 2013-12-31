import functools
import unittest

from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.client.hooks.hooked_api import NonHookedAuroraClientAPI


API_METHODS = ('cancel_update', 'create_job', 'kill_job', 'restart', 'start_cronjob', 'update_job')
API_METHODS_WITH_CONFIG_PARAM_ADDED = ('cancel_update', 'kill_job', 'restart', 'start_cronjob')


class TestNonHookedAuroraClientAPI(unittest.TestCase):
  """
    Tests that NonHookedAuroraClientAPI discards the 'config' param for all API method
    This is necessary so that the callsite can use either this or HookedAuroraClientAPI
    in a consistent fashion, while AuroraClientAPI does not receive that param
  """

  def __init__(self, *args, **kw):
    super(TestNonHookedAuroraClientAPI, self).__init__(*args, **kw)
    self.original_bases = {}

  def setUp(self):
    self.RETURN_VALUE = 'foo'
    test_obj = self

    class FakeAuroraClientAPI(object):

      def cancel_update(self, job_key):
        test_obj.API_CALL = functools.partial(self.cancel_update, job_key)
        return test_obj.RETURN_VALUE

      def kill_job(self, job_key, instances=None, lock=None):
        test_obj.API_CALL = functools.partial(self.kill_job, job_key, instances, lock)
        return test_obj.RETURN_VALUE

      def restart(self, job_key, shards, updater_config, health_check_interval_seconds):
        test_obj.API_CALL = functools.partial(self.restart, job_key, shards,
            updater_config, health_check_interval_seconds)
        return test_obj.RETURN_VALUE

      def start_cronjob(self, job_key):
        test_obj.API_CALL = functools.partial(self.start_cronjob, job_key)
        return test_obj.RETURN_VALUE

    self._patch_bases(NonHookedAuroraClientAPI, (FakeAuroraClientAPI, ))
    self.api = NonHookedAuroraClientAPI()

    # Test args passed in to check that these are proxied un-modified
    self.test_job_key = AuroraJobKey.from_path('a/b/c/d')
    self.test_config = 'bar'
    self.test_shards = 'baz'
    self.test_lock = 'lock'
    self.test_updater_config = 'blah'
    self.health_check_interval_seconds = 'baa'

  def tearDown(self):
    self._unpatch_bases(NonHookedAuroraClientAPI)

  def _patch_bases(self, cls, new_bases):
    self.original_bases[cls] = cls.__bases__
    cls.__bases__ = new_bases

  def _unpatch_bases(self, cls):
    cls.__bases__ = self.original_bases[cls]

  def _verify_api_call(self, return_value, *args, **kwargs):
    assert args == self.API_CALL.args
    assert kwargs == (self.API_CALL.keywords or {})
    assert return_value == self.RETURN_VALUE

  def test_cancel_update_discards_config(self):
    return_value = self.api.cancel_update(self.test_job_key, config=self.test_config)
    self._verify_api_call(return_value, self.test_job_key)

  def test_kill_job_discards_config(self):
    return_value = self.api.kill_job(
      self.test_job_key,
      self.test_shards,
      self.test_lock,
      config=self.test_config)
    self._verify_api_call(return_value, self.test_job_key, self.test_shards, self.test_lock)

  def test_restart_discards_config(self):
    return_value = self.api.restart(self.test_job_key, self.test_shards,
        self.test_updater_config, self.health_check_interval_seconds, config=self.test_config)
    self._verify_api_call(return_value, self.test_job_key, self.test_shards,
        self.test_updater_config, self.health_check_interval_seconds)

  def test_start_cronjob_discards_config(self):
    return_value = self.api.start_cronjob(self.test_job_key, config=self.test_config)
    self._verify_api_call(return_value, self.test_job_key)
