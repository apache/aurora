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

import getpass
import logging
import optparse
import os
import subprocess
from uuid import uuid1

from twitter.common.quantity import Amount, Time
from twitter.common.quantity.parse_simple import parse_time

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.base import AURORA_ADMIN_USER_AGENT_NAME, die
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import CLUSTERS

"""Admin client utility functions shared between admin and maintenance modules."""

# TODO(maxim): Switch to CLI ConfigurationPlugin within AURORA-486.
LOGGER_NAME = 'aurora_admin'
logger = logging.getLogger(LOGGER_NAME)
CLIENT_ID = uuid1()

# Default SLA limits
SLA_UPTIME_PERCENTAGE_LIMIT = 95
SLA_UPTIME_DURATION_LIMIT = Amount(30, Time.MINUTES)


def log_admin_message(sev, msg, *args, **kwargs):
  """Logs message using the module-defined logger.

   :param sev: message severity
   :type sev: The numeric level of the logging event (one of DEBUG, INFO etc.)
   :param msg: message to log
   :type msg: string
  """
  extra = kwargs.get('extra', {})
  extra['clientid'] = CLIENT_ID
  extra['user'] = getpass.getuser()
  extra['logger_name'] = LOGGER_NAME
  kwargs['extra'] = extra
  logger.log(sev, msg, *args, **kwargs)


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


POST_DRAIN_SCRIPT_OPTION = optparse.Option(
    '--post_drain_script',
    dest='post_drain_script',
    default=None,
    help='Path to a script to run for each host if needed.')

OVERRIDE_SLA_PERCENTAGE_OPTION = optparse.Option(
    '--override_percentage',
    dest='percentage',
    default=None,
    help='Percentage of tasks required to be up all the time within the duration. '
         'Default value:%s. DO NOT override default value unless absolutely necessary! '
         'See sla_probe_hosts and sla_list_safe_domain commands '
         'for more details on SLA.' % SLA_UPTIME_PERCENTAGE_LIMIT)

OVERRIDE_SLA_DURATION_OPTION = optparse.Option(
    '--override_duration',
    dest='duration',
    default=None,
    help='Time interval (now - value) for the percentage of up tasks. Format: XdYhZmWs. '
         'Default value:%s. DO NOT override default value unless absolutely necessary! '
         'See sla_probe_hosts and sla_list_safe_domain commands '
         'for more details on SLA.' % SLA_UPTIME_DURATION_LIMIT)

OVERRIDE_SLA_REASON_OPTION = optparse.Option(
    '--override_reason',
    dest='reason',
    default=None,
    help='Reason for overriding default SLA values. Provide details including the '
         'maintenance ticket number.')

DEFAULT_SLA_PERCENTAGE_OPTION = optparse.Option(
    '--default_percentage',
    dest='default_percentage',
    default=95,
    help='Percentage of tasks required to be up all the time within the duration. '
         'This percentage will be used for SLA calculation if tasks do not '
         'have a configured SlaPolicy.')

DEFAULT_SLA_DURATION_OPTION = optparse.Option(
    '--default_duration',
    dest='default_duration',
    default='30m',
    help='Time interval (now - value) for the percentage of up tasks. Format: XdYhZmWs. '
         'This duration will be used for SLA calculation if tasks do not '
         'have a configured SlaPolicy.')

FORCE_DRAIN_TIMEOUT_OPTION = optparse.Option(
    '--force_drain_timeout',
    dest='timeout',
    default='7d',
    help='Time interval (now - value) after which tasks will be forcefully drained. '
         'Format: XdYhZmWs.')

UNSAFE_SLA_HOSTS_FILE_OPTION = optparse.Option(
    '--unsafe_hosts_file',
    dest='unsafe_hosts_filename',
    default=None,
    help='Output file to write host names that did not pass SLA check.')


def parse_sla_percentage(percentage):
  """Parses percentage value for an SLA check.

  :param percentage: string percentage to parse
  :type percentage: string
  :rtype: float
  """
  val = float(percentage)
  if val <= 0 or val > 100:
    die('Invalid percentage %s. Must be within (0, 100].' % percentage)
  return val


def _parse_hostname_list(hostname_list):
  hostnames = [hostname.strip() for hostname in hostname_list.split(",")]
  if not hostnames:
    die('No valid hosts found.')
  return hostnames


def _parse_hostname_file(filename):
  with open(filename, 'r') as hosts:
    hostnames = [hostname.strip() for hostname in hosts]
  if not hostnames:
    die('No valid hosts found in %s.' % filename)
  return hostnames


def parse_hostnames_optional(list_option, file_option):
  """Parses host names from a comma-separated list or a filename.

  Does not require either of the arguments (returns None list if no option provided).

  :param list_option: command option with comma-separated list of host names
  :type list_option: app.option
  :param file_option: command option with filename (one host per line)
  :type file_option: app.option
  :rtype: list of host names or None.
  """
  if bool(list_option) and bool(file_option):
    die('Cannot specify both filename and list for the same option.')
  hostnames = None
  if file_option:
    hostnames = _parse_hostname_file(file_option)
  elif list_option:
    hostnames = _parse_hostname_list(list_option)
  return hostnames


def parse_hostnames(filename, hostnames):
  """Parses host names from a comma-separated list or a filename.

  Fails if neither filename nor hostnames provided.

  :param filename: filename with host names (one per line)
  :type filename: string
  :param hostnames: comma-separated list of host names
  :type hostnames: string
  :rtype: list of host names
  """
  if bool(filename) == bool(hostnames):
    die('Please specify either --filename or --hosts')
  if filename:
    hostnames = _parse_hostname_file(filename)
  elif hostnames:
    hostnames = _parse_hostname_list(hostnames)
  if not hostnames:
    die('No valid hosts found.')
  return hostnames


def parse_script(filename):
  """Parses shell script from the provided file and wraps it up into a subprocess callback.

  :param filename: name of the script file
  :type filename: string
  :rtype: function
  """
  def callback(host):
    subprocess.Popen([cmd, host]).wait()

  if filename:
    if not os.path.exists(filename):
      die("No such file: %s" % filename)
    cmd = os.path.abspath(filename)
    return callback
  else:
    return None


def parse_and_validate_sla_overrides(options, hostnames):
  """Parses and validates host SLA override 3-tuple (percentage, duration, reason).

  In addition, logs an admin message about overriding default SLA values.

  :param options: command line options
  :type options: list of app.option
  :param hostnames: host names override is issued to
  :type hostnames: list of string
  :rtype: a tuple of: override percentage (float) and override duration (Amount)
  """
  has_override = bool(options.percentage) or bool(options.duration) or bool(options.reason)
  all_overrides = bool(options.percentage) and bool(options.duration) and bool(options.reason)
  if has_override != all_overrides:
    die('All --override_* options are required when attempting to override default SLA values.')

  print(options.percentage)
  percentage = parse_sla_percentage(options.percentage) if options.percentage else None
  duration = parse_time(options.duration) if options.duration else None
  if options.reason:
    log_admin_message(
      logging.WARNING,
      'Default SLA values (percentage: %s, duration: %s) are overridden for the following '
      'hosts: %s. New percentage: %s, duration: %s, override reason: %s' % (
        SLA_UPTIME_PERCENTAGE_LIMIT,
        SLA_UPTIME_DURATION_LIMIT,
        hostnames,
        percentage,
        duration,
        options.reason))

  return percentage or SLA_UPTIME_PERCENTAGE_LIMIT, duration or SLA_UPTIME_DURATION_LIMIT


def parse_and_validate_sla_drain_default(options):
  """Parses and validates host SLA default 3-tuple (percentage, duration, timeout).

  :param options: command line options
  :type options: list of app.option
  :rtype: a tuple of: default percentage (float), default duration (Amount) and timeout (Amount)
  """
  percentage = parse_sla_percentage(options.default_percentage)
  duration = parse_time(options.default_duration).as_(Time.SECONDS)
  timeout = parse_time(options.timeout).as_(Time.SECONDS)

  return percentage, duration, timeout


def print_results(results):
  """Prints formatted SLA results.

  :param results: formatted SLA results
  :type results: list of string
  """
  for line in results:
    print(line)


def format_sla_results(host_groups, unsafe_only=False):
  """Formats SLA check result output.

  :param host_groups: SLA check result groups (grouped by external grouping criteria, e.g. by_host)
  :type host_groups: list of (defaultdict(list))
  :param unsafe_only: If True, includes only SLA-"unsafe" hosts from the results
  :type unsafe_only: bool
  :rtype: a tuple of: list of output strings, set of hostnames included in output.
  """
  results = []
  include_unsafe_only = lambda d: not d.safe if unsafe_only else True

  hostnames = set()
  for group in host_groups:
    for host, job_details in sorted(group.items()):
      host_details = '\n'.join(
          ['%s\t%s\t%.2f\t%s\t%s' %
              (host,
               d.job.to_path(),
               d.predicted_percentage,
               d.safe,
               'n/a' if d.safe_in_secs is None else d.safe_in_secs)
              for d in sorted(job_details) if include_unsafe_only(d)])
      if host_details:
        results.append(host_details)
        hostnames.add(host)
  return results, hostnames


def make_admin_client(cluster, verbose=False, bypass_leader_redirect=False):
  """Creates an API client with the specified options for use in admin commands.

  :param cluster: The cluster to connect with.
  :type cluster: Either a string cluster name or a Cluster object.
  :param verbose: Should the client emit verbose output.
  :type verbose: bool
  :type bypass_leader_redirect: Should the client bypass the scheduler's leader redirect filter.
  :type bypass_leader_redirect: bool
  :rtype: an AuroraClientAPI instance.
  """

  is_cluster_object = isinstance(cluster, Cluster)

  if not is_cluster_object and cluster not in CLUSTERS:
    die('Unknown cluster: %s. Known clusters: %s' % (cluster, ", ".join(CLUSTERS.keys())))

  return AuroraClientAPI(
      cluster if is_cluster_object else CLUSTERS[cluster],
      AURORA_ADMIN_USER_AGENT_NAME,
      verbose=verbose,
      bypass_leader_redirect=bypass_leader_redirect)
