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
import optparse
import sys
from collections import defaultdict
from urlparse import urljoin

from twitter.common import log

from apache.aurora.common.pex_version import pex_version, UnknownVersion

from gen.apache.aurora.api.ttypes import ResponseCode


LOCKED_WARNING = """
Note: if the scheduler detects that a job update is in progress (or was not
properly completed) it will reject subsequent updates.  This is because your
job is likely in a partially-updated state.  You should only begin another
update if you are confident that nobody is updating this job, and that
the job is in a state suitable for an update.

After checking on the above, you may release the update lock on the job by
invoking cancel_update.
"""


def die(msg):
  log.fatal(msg)
  sys.exit(1)


def combine_messages(response):
  """Combines the message found in the details of a response.
  :param response: response to extract messages from.
  :return: Messages from the details in the response, or an empty string if there were no messages.
  """
  return ', '.join([d.message or 'Unknown error' for d in (response.details or [])])


def format_response(resp):
  return 'Response from scheduler: %s (message: %s)' % (
    ResponseCode._VALUES_TO_NAMES[resp.responseCode], combine_messages(resp))


def check_and_log_response(resp):
  log.info(format_response(resp))
  if resp.responseCode != ResponseCode.OK:
    if resp.responseCode == ResponseCode.LOCK_ERROR:
      log.info(LOCKED_WARNING)
    sys.exit(1)


def check_and_log_locked_response(resp):
  if resp.responseCode == ResponseCode.LOCK_ERROR:
    log.info(LOCKED_WARNING)


def get_populated_task_config(resp):
  """Temporary dual read until AURORA-975 is addressed."""
  result = resp.result.populateJobResult
  if result.taskConfig:
    return result.taskConfig
  else:
    return list(result.populatedDEPRECATED)[0] if result.populatedDEPRECATED else None


def deprecation_warning(text):
  log.warning('')
  log.warning('*' * 80)
  log.warning('* The command you ran is deprecated and will soon break!')
  for line in text.split('\n'):
    log.warning('* %s' % line)
  log.warning('*' * 80)
  log.warning('')


class requires(object):  # noqa
  @classmethod
  def wrap_function(cls, fn, fnargs, comparator):
    @functools.wraps(fn)
    def wrapped_function(args):
      if not comparator(args, fnargs):
        help = 'Incorrect parameters for %s' % fn.__name__
        if fn.__doc__:
          help = '%s\n\nsee the help subcommand for more details.' % fn.__doc__.split('\n')[0]
        die(help)
      return fn(*args)
    return wrapped_function

  @classmethod
  def exactly(cls, *args):
    def wrap(fn):
      return cls.wrap_function(fn, args, (lambda want, got: len(want) == len(got)))
    return wrap

  @classmethod
  def at_least(cls, *args):
    def wrap(fn):
      return cls.wrap_function(fn, args, (lambda want, got: len(want) >= len(got)))
    return wrap

  @classmethod
  def nothing(cls, fn):
    @functools.wraps(fn)
    def real_fn(line):
      return fn(*line)
    return real_fn


def group_by_host(hostname):
  return hostname


DEFAULT_GROUPING = 'by_host'
GROUPING_FUNCTIONS = {
    'by_host': group_by_host,
}


def add_grouping(name, function):
  GROUPING_FUNCTIONS[name] = function


def remove_grouping(name):
  GROUPING_FUNCTIONS.pop(name)


def get_grouping_or_die(grouping_function):
  try:
    return GROUPING_FUNCTIONS[grouping_function]
  except KeyError:
    die('Unknown grouping function %s. Must be one of: %s'
        % (grouping_function, GROUPING_FUNCTIONS.keys()))


def group_hosts(hostnames, grouping_function=DEFAULT_GROUPING):
  """Place a list of hosts into batches to be operated upon.

  By default, the grouping function is 'by host' which means that maintenance will
  operate on a single hostname at a time. By adding more grouping functions,
  a site can setup a customized way of specifying groups, such as operating on a single
  rack of hosts at a time.

  :param hostnames: Hostnames to break into groups
  :type hostnames: list of host names, must match the host names that slaves are registered with
  :param grouping_function: Key within GROUPING_FUNCTIONS to partition hosts into desired batches
  :type grouping_function: string
  :rtype: dictionary of batches
  """
  grouping_function = get_grouping_or_die(grouping_function)
  groups = defaultdict(set)
  for hostname in hostnames:
    groups[grouping_function(hostname)].add(hostname)
  return groups


GROUPING_OPTION = optparse.Option(
    '--grouping',
    type='string',
    metavar='GROUPING',
    default=DEFAULT_GROUPING,
    dest='grouping',
    help='Grouping function to use to group hosts.  Options: %s.  Default: %%default' % (
        ', '.join(GROUPING_FUNCTIONS.keys())))


def synthesize_url(scheduler_url, role=None, env=None, job=None):
  if not scheduler_url:
    log.warning("Unable to find scheduler web UI!")
    return None

  if env and not role:
    die('If env specified, must specify role')
  if job and not (role and env):
    die('If job specified, must specify role and env')

  scheduler_url = urljoin(scheduler_url, 'scheduler')
  if role:
    scheduler_url += '/' + role
    if env:
      scheduler_url += '/' + env
      if job:
        scheduler_url += '/' + job
  return scheduler_url


AURORA_V2_USER_AGENT_NAME = 'Aurora V2'
AURORA_ADMIN_USER_AGENT_NAME = 'Aurora Admin'

UNKNOWN_CLIENT_VERSION = 'Unknown Version'


def user_agent(agent_name='Aurora'):
  """Generate a user agent containing the specified agent name and the details of the current
     client version."""
  try:
    build_info = '%s-%s' % pex_version(sys.argv[0])
  except UnknownVersion:
    build_info = UNKNOWN_CLIENT_VERSION

  return '%s;%s' % (agent_name, build_info)
