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

import functools
import traceback

from twitter.common import log

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.base import combine_messages
from apache.aurora.client.config import GlobalHookRegistry

from gen.apache.aurora.api.ttypes import ResponseCode


def _partial(function, *args, **kw):
  """Returns a partial function __name__ inherited from parent function."""
  partial = functools.partial(function, *args, **kw)
  return functools.update_wrapper(partial, function)


class HookConfig(object):
  def __init__(self, config, job_key):
    self.config = config
    self.job_key = job_key or (config.job_key() if config is not None else None)

  def __iter__(self):
    yield self.config
    yield self.job_key


class NonHookedAuroraClientAPI(AuroraClientAPI):
  """
    This wraps those AuroraClientAPI methods that don't have an AuroraConfig 'config' param
    to take an optional 'config' param which:
    * contains the configured hooks (config.hooks)
    * is dropped before the call is proxied to AuroraClientAPI
    * is thus available to API methods in subclasses
  """

  def kill_job(self, job_key, instances=None, lock=None, config=None):
    return super(NonHookedAuroraClientAPI, self).kill_job(job_key, instances=instances, lock=lock)

  def restart(self, job_key, shards, updater_config, health_check_interval_seconds, config=None):
    return super(NonHookedAuroraClientAPI, self).restart(job_key, shards, updater_config,
        health_check_interval_seconds)

  def start_cronjob(self, job_key, config=None):
    return super(NonHookedAuroraClientAPI, self).start_cronjob(job_key)


class HookedAuroraClientAPI(NonHookedAuroraClientAPI):
  """
    Adds a hooking aspect/behaviour to the lifecycle of Mesos Client API methods
    by injecting hooks (instances of apache.aurora.client.hooks.Hooks)

    * Hooks are available in the 'config' (AuroraConfig) param that each API call receives
    * Each Hook is run around each API call:
      * 'pre' hook before the call
      * 'post' hook if the call succeeds
      * 'err' hook if the call fails
    * If the hook itself fails, then it is treated as a WARN rather than an ERROR
  """

  class Error(Exception): pass
  class PreHooksStoppedCall(Error): pass
  class APIError(Error):
    def __init__(self, response):
      self.response = response

    def __str__(self):
      return '%s: %s: %s' % (self.__class__.__name__,
          ResponseCode._VALUES_TO_NAMES.get(self.response.responseCode, 'UNKNOWN'),
          combine_messages(self.response))

  @classmethod
  def _meta_hook(cls, hook, hook_method):
    def callback():
      if hook_method is None:
        return True
      log.debug('Running %s in %s' % (hook_method.__name__, hook.__class__.__name__))
      hook_result = False
      try:
        hook_result = hook_method()
        if not hook_result:
          log.debug('%s in %s returned False' % (hook_method.__name__,
              hook.__class__.__name__))
      except Exception:
        log.warn('Error in %s in %s' %
            (hook_method.__name__, hook.__class__.__name__))
        log.warn(traceback.format_exc())
      return hook_result
    return callback

  @classmethod
  def _generate_method(cls, hook, config, job_key, event, method, extra_argument=None):
    method_name, args, kw = method.__name__, method.args, method.keywords
    kw = kw or {}
    hook_method = getattr(hook, '%s_%s' % (event, method_name), None)
    if callable(hook_method):
      if extra_argument is not None:
        hook_method = _partial(hook_method, extra_argument)
      return _partial(hook_method, *args, **kw)
    else:
      hook_method = getattr(hook, 'generic_hook', None)
      if hook_method is None:
        return None
      hook_method = _partial(hook_method, HookConfig(config, job_key),
          event, method_name, extra_argument)
      return _partial(hook_method, args, kw)

  @classmethod
  def _yield_hooks(cls, event, config, job_key, api_call, extra_argument=None):
    hooks = GlobalHookRegistry.get_hooks()
    hooks += (config.hooks if config and config.raw().enable_hooks().get() else [])
    for hook in hooks:
      yield cls._meta_hook(hook,
          cls._generate_method(hook, config, job_key, event, api_call, extra_argument))

  @classmethod
  def _invoke_hooks(cls, event, config, job_key, api_call, extra_argument=None):
    hooks_passed = [hook() for hook in cls._yield_hooks(event, config, job_key, api_call,
        extra_argument)]
    return all(hooks_passed)

  def _hooked_call(self, config, job_key, api_call):
    if not self._invoke_hooks('pre', config, job_key, api_call):
      raise self.PreHooksStoppedCall('Pre hooks stopped call to %s' % api_call.__name__)

    try:
      resp = api_call()
    except Exception as e:
      self._invoke_hooks('err', config, job_key, api_call, e)
      raise  # propagate since the API method call failed for unknown reasons

    if resp.responseCode != ResponseCode.OK:
      self._invoke_hooks('err', config, job_key, api_call, self.APIError(resp))
    else:
      self._invoke_hooks('post', config, job_key, api_call, resp)

    return resp

  def create_job(self, config, lock=None):
    return self._hooked_call(config, None,
        _partial(super(HookedAuroraClientAPI, self).create_job, config, lock))

  def kill_job(self, job_key, instances=None, lock=None, config=None):
    return self._hooked_call(config, job_key,
        _partial(super(HookedAuroraClientAPI, self).kill_job,
            job_key, instances=instances, lock=lock, config=config))

  def restart(self, job_key, shards, updater_config, health_check_interval_seconds, config=None):
    return self._hooked_call(config, job_key,
        _partial(super(HookedAuroraClientAPI, self).restart,
            job_key, shards, updater_config, health_check_interval_seconds, config=config))

  def start_cronjob(self, job_key, config=None):
    return self._hooked_call(config, job_key,
        _partial(super(HookedAuroraClientAPI, self).start_cronjob,
            job_key, config=config))

  def start_job_update(self, config, message, instances=None):
    return self._hooked_call(config, None,
        _partial(super(HookedAuroraClientAPI, self).start_job_update,
            config, message, instances=instances))
