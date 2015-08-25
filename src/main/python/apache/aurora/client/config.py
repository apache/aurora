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

'''Library of utilities called by the aurora client binary
'''

from __future__ import print_function

import functools
import math
import re
import sys

from pystachio import Empty

from apache.aurora.client import binding_helper
from apache.aurora.client.base import die
from apache.aurora.config import AuroraConfig


ANNOUNCE_WARNING = """
Announcer specified primary port as '%(primary_port)s' but no processes have bound that port.
If you would like to utilize this port, you should listen on {{thermos.ports[%(primary_port)s]}}
from some Process bound to your task.
"""


def _validate_announce_configuration(config):
  if not config.raw().has_announce():
    return

  primary_port = config.raw().announce().primary_port().get()
  if primary_port not in config.ports():
    print(ANNOUNCE_WARNING % {'primary_port': primary_port}, file=sys.stderr)

  if config.raw().has_announce() and not config.raw().has_constraints() or (
      'dedicated' not in config.raw().constraints()):
    for port in config.raw().announce().portmap().get().values():
      try:
        port = int(port)
      except ValueError:
        continue
      raise ValueError('Job must be dedicated in order to specify static ports!')


STAGING_RE = re.compile(r'^staging\d*$')


#TODO(maxim): Merge env and tier and move definitions to scheduler: AURORA-1443.
def __validate_env(name, config_name):
  if STAGING_RE.match(name):
    return
  if name not in ('prod', 'devel', 'test'):
    raise ValueError('%s should be one of "prod", "devel", "test" or '
                     'staging<number>!  Got %s' % (config_name, name))


def _validate_environment_name(config):
  env_name = str(config.raw().environment())
  __validate_env(env_name, 'Environment')


def _validate_tier(config):
  tier_raw = config.raw().tier()
  tier_name = str(tier_raw) if tier_raw is not Empty else None
  if tier_name is not None:
    __validate_env(tier_name, 'Tier')


UPDATE_CONFIG_MAX_FAILURES_ERROR = '''
max_total_failures in update_config must be lesser than the job size.
Based on your job size (%s) you should use max_total_failures <= %s.
'''


UPDATE_CONFIG_DEDICATED_THRESHOLD_ERROR = '''
Since this is a dedicated job, you must set your max_total_failures in
your update configuration to no less than 2%% of your job size.
Based on your job size (%s) you should use max_total_failures >= %s.
'''


WATCH_SECS_INSUFFICIENT_ERROR_FORMAT = '''
You have specified an insufficiently short watch period (%d seconds) in your update configuration.
Your update will always succeed. In order for the updater to detect health check failures,
UpdateConfig.watch_secs must be greater than %d seconds to account for an initial
health check interval (%d seconds) plus %d consecutive failures at a check interval of %d seconds.
'''


def _validate_update_config(config):
  job_size = config.instances()
  update_config = config.update_config()
  health_check_config = config.health_check_config()

  max_failures = update_config.max_total_failures().get()
  watch_secs = update_config.watch_secs().get()
  initial_interval_secs = health_check_config.initial_interval_secs().get()
  max_consecutive_failures = health_check_config.max_consecutive_failures().get()
  interval_secs = health_check_config.interval_secs().get()

  if max_failures >= job_size:
    die(UPDATE_CONFIG_MAX_FAILURES_ERROR % (job_size, job_size - 1))

  if config.is_dedicated():
    min_failure_threshold = int(math.floor(job_size * 0.02))
    if max_failures < min_failure_threshold:
      die(UPDATE_CONFIG_DEDICATED_THRESHOLD_ERROR % (job_size, min_failure_threshold))

  target_watch = initial_interval_secs + (max_consecutive_failures * interval_secs)
  if watch_secs <= target_watch:
    die(WATCH_SECS_INSUFFICIENT_ERROR_FORMAT %
        (watch_secs, target_watch, initial_interval_secs, max_consecutive_failures, interval_secs))


def validate_config(config, env=None):
  _validate_update_config(config)
  _validate_announce_configuration(config)
  _validate_environment_name(config)
  _validate_tier(config)


class GlobalHookRegistry(object):
  """To allow enforcable policy, we need a set of mandatory hooks that are
  registered as part of the client executable. Global hooks are registered
  by calling GlobalHookRegistry.register_global_hook.
  """

  HOOKS = []

  DISABLED = False

  @classmethod
  def register_global_hook(cls, hook):
    cls.HOOKS.append(hook)

  @classmethod
  def get_hooks(cls):
    if cls.DISABLED:
      return []
    else:
      return cls.HOOKS[:]

  @classmethod
  def reset(cls):
    cls.HOOKS = []
    cls.DISABLED = False

  @classmethod
  def disable_hooks(cls):
    cls.DISABLED = True

  @classmethod
  def enable_hooks(cls):
    cls.DISABLED = False


def inject_hooks(config, env=None):
  config.hooks = (env or {}).get('hooks', [])


class AnnotatedAuroraConfig(AuroraConfig):
  @classmethod
  def plugins(cls):
    return (inject_hooks,
            functools.partial(binding_helper.apply_all),
            validate_config)


def get_config(jobname,
               config_file,
               json=False,
               bindings=(),
               select_cluster=None,
               select_role=None,
               select_env=None):
  """Creates and returns a config object contained in the provided file."""
  loader = AnnotatedAuroraConfig.load_json if json else AnnotatedAuroraConfig.load
  return loader(config_file,
                jobname,
                bindings,
                select_cluster=select_cluster,
                select_role=select_role,
                select_env=select_env)
