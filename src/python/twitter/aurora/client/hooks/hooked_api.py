import functools
import traceback

from twitter.common import log
from twitter.aurora.client.api import AuroraClientAPI
from twitter.aurora.common import AuroraJobKey

from gen.twitter.aurora.ttypes import ResponseCode

from . import JoinPoint


class NonHookedAuroraClientAPI(AuroraClientAPI):
  """
    This wraps the AuroraClientAPI methods and certain methods accept an extra 'config' param
    that is not passed forward to the API method.
    This config param contains the configured hooks.
  """

  def cancel_update(self, job_key, config=None):
    return super(NonHookedAuroraClientAPI, self).cancel_update(job_key)

  def kill_job(self, job_key, shards=None, config=None):
    return super(NonHookedAuroraClientAPI, self).kill_job(job_key, shards=shards)

  def restart(self, job_key, shards, updater_config, health_check_interval_seconds, config=None):
    return super(NonHookedAuroraClientAPI, self).restart(job_key, shards, updater_config,
        health_check_interval_seconds)

  def start_cronjob(self, job_key, config=None):
    return super(NonHookedAuroraClientAPI, self).start_cronjob(job_key)


class HookedAuroraClientAPI(NonHookedAuroraClientAPI):
  """
    Adds a hooking aspect/behaviour to the lifecycle of Mesos Client API methods
    by injecting hooks (instances of twitter.aurora.client.hooks.Hooks)

    * It wraps each API method to have:
      * a 'pre' hook before execution
      * a 'post' hook after successful execution
      * a 'err' hook after unsuccessful execution
    * It captures the requested hooks available in config.hooks
    * It allows updating the hooks list using hooks_list_updater
    * It runs these hooks (pre, post/err) for each API call - this is the aspect
  """

  class Error(Exception): pass
  class PreHooksStoppedCall(Error): pass
  class APIError(Error):
    def __init__(self, response):
      self.response = response

    def __str__(self):
      return '%s: %s: %s' % (self.__class__.__name__,
          ResponseCode._VALUES_TO_NAMES.get(self.response.responseCode, 'UNKNOWN'),
          self.response.message)

  def _hooked_call(self, config, job_key, api_call):
    pre, post, err = [JoinPoint(time, api_call.func.__name__) for time in JoinPoint.TIMES]

    if config and not job_key:
      job_key = AuroraJobKey(config.cluster(), config.role(),
          config.environment(), config.name())

    proceed = self._call_all_hooks(pre, api_call, None, config, job_key)
    if not proceed:
      raise self.PreHooksStoppedCall('Pre hooks stopped call to %s' % api_call.func.__name__)

    try:
      resp = api_call()
    except Exception as e:
      self._call_all_hooks(err, api_call, e, config, job_key)
      raise  # propagate since the API method call failed for unknown reasons

    if resp.responseCode != ResponseCode.OK:
      self._call_all_hooks(err, api_call, self.APIError(resp), config, job_key)
    else:
      self._call_all_hooks(post, api_call, resp, config, job_key)

    return resp

  def _call_all_hooks(self, join_point, api_call, result_or_err, config, job_key):
    hooks = config.hooks if config else []
    for hook in hooks:
      # TODO(sgeorge): AWESOME-4752: Call hooks in an async manner
      log.debug('Running %s in %s' % (join_point.name(), hook.__class__.__name__))
      try:
        hook_method = getattr(hook, join_point.name())
        hook_result = hook_method(job_key, result_or_err, api_call.args, api_call.keywords)
        if not hook_result:
          log.debug('%s in %s returned False' % (repr(join_point), hook.__class__.__name__))
          return False
      except Exception:
        log.warn('Error in %s in %s' %
            (join_point.name(), hook.__class__.__name__))
        log.warn(traceback.format_exc())
    return True  # None of the pre hooks returned False

  def create_job(self, config):
    return self._hooked_call(config, None,
        functools.partial(super(HookedAuroraClientAPI, self).create_job, config))

  def cancel_update(self, job_key, config=None):
    return self._hooked_call(config, job_key,
        functools.partial(super(HookedAuroraClientAPI, self).cancel_update,
            job_key, config=config))

  def kill_job(self, job_key, shards=None, config=None):
    return self._hooked_call(config, job_key,
        functools.partial(super(HookedAuroraClientAPI, self).kill_job,
            job_key, shards=shards, config=config))

  def restart(self, job_key, shards, updater_config, health_check_interval_seconds, config=None):
    return self._hooked_call(config, job_key,
        functools.partial(super(HookedAuroraClientAPI, self).restart,
            job_key, shards, updater_config, health_check_interval_seconds, config=config))

  def start_cronjob(self, job_key, config=None):
    return self._hooked_call(config, job_key,
        functools.partial(super(HookedAuroraClientAPI, self).start_cronjob,
            job_key, config=config))

  def update_job(self, config, health_check_interval_seconds=3, shards=None):
    return self._hooked_call(config, None,
        functools.partial(super(HookedAuroraClientAPI, self).update_job,
            config, health_check_interval_seconds=health_check_interval_seconds, shards=shards))
