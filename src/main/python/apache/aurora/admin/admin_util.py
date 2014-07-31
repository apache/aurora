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

from apache.aurora.client.base import die

"""Admin client utility functions shared between admin and maintenance modules."""

# TODO(maxim): Switch to CLI ConfigurationPlugin within AURORA-486.
LOGGER_NAME = 'aurora_admin'
logger = logging.getLogger(LOGGER_NAME)
CLIENT_ID = uuid1()


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
