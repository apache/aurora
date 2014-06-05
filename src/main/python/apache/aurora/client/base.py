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

from twitter.common import app, log

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


def log_response(resp):
  log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.messageDEPRECATED))


def check_and_log_response(resp):
  log_response(resp)
  if resp.responseCode != ResponseCode.OK:
    if resp.responseCode == ResponseCode.LOCK_ERROR:
      log.info(LOCKED_WARNING)
    sys.exit(1)


def check_and_log_locked_response(resp):
  if resp.responseCode == ResponseCode.LOCK_ERROR:
    log.info(LOCKED_WARNING)


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


FILENAME_OPTION = optparse.Option(
    '--filename',
    dest='filename',
    default=None,
    help='Name of the file with hostnames')


HOSTS_OPTION = optparse.Option(
    '--hosts',
    dest='hosts',
    default=None,
    help='Comma separated list of hosts')


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


def parse_host_list(host_list):
  hosts = [hostname.strip() for hostname in host_list.split(",")]
  if not hosts:
    die('No valid hosts found.')
  return hosts


def parse_host_file(filename):
  with open(filename, 'r') as hosts:
    hosts = [hostname.strip() for hostname in hosts]
  if not hosts:
    die('No valid hosts found in %s.' % filename)
  return hosts


def parse_hosts_optional(list_option, file_option):
  if bool(list_option) and bool(file_option):
    die('Cannot specify both filename and list for the same option.')
  hosts = None
  if file_option:
    hosts = parse_host_file(file_option)
  elif list_option:
    hosts = parse_host_list(list_option)
  return hosts


def parse_hosts(filename, hosts):
  if bool(filename) == bool(hosts):
    die('Please specify either --filename or --hosts')
  if filename:
    hosts = parse_host_file(filename)
  elif hosts:
    hosts = parse_host_list(hosts)
  if not hosts:
    die('No valid hosts found.')
  return hosts


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


def handle_open(scheduler_url, role, env, job):
  url = synthesize_url(scheduler_url, role, env, job)
  if url:
    log.info('Job url: %s' % url)
    if app.get_options().open_browser:
      import webbrowser
      webbrowser.open_new_tab(url)


def make_commands_str(command_aliases):
  """Format a string representation of a number of command aliases."""
  commands = command_aliases[:]
  commands.sort()
  if len(commands) == 1:
    return str(commands[0])
  elif len(commands) == 2:
    return '%s (or %s)' % (str(commands[0]), str(commands[1]))
  else:
    return '%s (or any of: %s)' % (str(commands[0]), ' '.join(map(str, commands[1:])))


# TODO(wickman) This likely belongs in twitter.common.app (or split out as
# part of a possible twitter.common.cli)
def generate_full_usage():
  """Generate verbose application usage from all registered
     twitter.common.app commands and return as a string."""
  docs_to_commands = defaultdict(list)
  for (command, doc) in app.get_commands_and_docstrings():
    docs_to_commands[doc].append(command)
  def make_docstring(item):
    (doc_text, commands) = item
    def format_line(line):
      return '    %s\n' % line.lstrip()
    stripped = ''.join(map(format_line, doc_text.splitlines()))
    return '%s\n%s' % (make_commands_str(commands), stripped)
  usage = sorted(map(make_docstring, docs_to_commands.items()))
  return 'Available commands:\n\n' + '\n'.join(usage)


def generate_terse_usage():
  """Generate minimal application usage from all registered
     twitter.common.app commands and return as a string."""
  docs_to_commands = defaultdict(list)
  for (command, doc) in app.get_commands_and_docstrings():
    docs_to_commands[doc].append(command)
  usage = '\n    '.join(sorted(map(make_commands_str, docs_to_commands.values())))
  return """
Available commands:
    %s

For more help on an individual command:
    %s help <command>
""" % (usage, app.name())
