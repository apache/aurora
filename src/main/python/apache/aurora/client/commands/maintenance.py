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

import logging

from twitter.common import app, log
from twitter.common.quantity.parse_simple import parse_time

from apache.aurora.admin.admin_util import (
    FILENAME_OPTION,
    HOSTS_OPTION,
    log_admin_message,
    parse_hostnames,
    parse_script,
    parse_sla_percentage
)
from apache.aurora.admin.host_maintenance import HostMaintenance
from apache.aurora.client.base import die, get_grouping_or_die, GROUPING_OPTION, requires
from apache.aurora.common.clusters import CLUSTERS


@app.command
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@requires.exactly('cluster')
def start_maintenance_hosts(cluster):
  """usage: start_maintenance_hosts {--filename=filename | --hosts=hosts}
                                    cluster
  """
  options = app.get_options()
  HostMaintenance(CLUSTERS[cluster], options.verbosity).start_maintenance(
      parse_hostnames(options.filename, options.hosts))


@app.command
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@requires.exactly('cluster')
def end_maintenance_hosts(cluster):
  """usage: end_maintenance_hosts {--filename=filename | --hosts=hosts}
                                  cluster
  """
  options = app.get_options()
  HostMaintenance(CLUSTERS[cluster], options.verbosity).end_maintenance(
      parse_hostnames(options.filename, options.hosts))


@app.command
@app.command_option('--post_drain_script', dest='post_drain_script', default=None,
    help='Path to a script to run for each host.')
@app.command_option('--override_percentage', dest='percentage', default=None,
    help='Percentage of tasks required to be up all the time within the duration. '
         'Default value:%s. DO NOT override default value unless absolutely necessary! '
         'See sla_probe_hosts and sla_list_safe_domain commands '
         'for more details on SLA.' % HostMaintenance.SLA_UPTIME_PERCENTAGE_LIMIT)
@app.command_option('--override_duration', dest='duration', default=None,
    help='Time interval (now - value) for the percentage of up tasks. Format: XdYhZmWs. '
         'Default value:%s. DO NOT override default value unless absolutely necessary! '
         'See sla_probe_hosts and sla_list_safe_domain commands '
         'for more details on SLA.' % HostMaintenance.SLA_UPTIME_DURATION_LIMIT)
@app.command_option('--override_reason', dest='reason', default=None,
    help='Reason for overriding default SLA values.')
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@app.command_option(GROUPING_OPTION)
@requires.exactly('cluster')
def perform_maintenance_hosts(cluster):
  """usage: perform_maintenance_hosts {--filename=filename | --hosts=hosts}
                                      [--post_drain_script=path]
                                      [--grouping=function]
                                      [--override_percentage=percentage]
                                      [--override_duration=duration]
                                      [--override_reason=reason]
                                      cluster

  Asks the scheduler to remove any running tasks from the machine and remove it
  from service temporarily, perform some action on them, then return the machines
  to service.
  """
  options = app.get_options()
  drainable_hosts = parse_hostnames(options.filename, options.hosts)
  get_grouping_or_die(options.grouping)

  has_override = bool(options.percentage) or bool(options.duration) or bool(options.reason)
  all_overrides = bool(options.percentage) and bool(options.duration) and bool(options.reason)
  if has_override != all_overrides:
    die('All --override_* options are required when attempting to override default SLA values.')

  percentage = parse_sla_percentage(options.percentage) if options.percentage else None
  duration = parse_time(options.duration) if options.duration else None
  if options.reason:
    log_admin_message(logging.WARNING, options.reason)

  drained_callback = parse_script(options.post_drain_script)

  HostMaintenance(CLUSTERS[cluster], options.verbosity).perform_maintenance(
      drainable_hosts,
      grouping_function=options.grouping,
      callback=drained_callback,
      percentage=percentage,
      duration=duration)


@app.command
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@requires.exactly('cluster')
def host_maintenance_status(cluster):
  """usage: host_maintenance_status {--filename=filename | --hosts=hosts}
                                    cluster

  Check on the schedulers maintenance status for a list of hosts in the cluster.
  """
  options = app.get_options()
  checkable_hosts = parse_hostnames(options.filename, options.hosts)
  statuses = HostMaintenance(CLUSTERS[cluster], options.verbosity).check_status(checkable_hosts)
  for pair in statuses:
    log.info("%s is in state: %s" % pair)
