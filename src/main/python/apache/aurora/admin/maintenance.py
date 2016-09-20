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

from twitter.common import app, log

from apache.aurora.client.base import GROUPING_OPTION, get_grouping_or_die, requires
from apache.aurora.common.clusters import CLUSTERS

from .admin_util import (
    FILENAME_OPTION,
    HOSTS_OPTION,
    OVERRIDE_SLA_DURATION_OPTION,
    OVERRIDE_SLA_PERCENTAGE_OPTION,
    OVERRIDE_SLA_REASON_OPTION,
    POST_DRAIN_SCRIPT_OPTION,
    UNSAFE_SLA_HOSTS_FILE_OPTION,
    parse_and_validate_sla_overrides,
    parse_hostnames,
    parse_script
)
from .host_maintenance import HostMaintenance


# TODO(maxim): merge with admin.py commands.
@app.command
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@requires.exactly('cluster')
def host_deactivate(cluster):
  """usage: host_deactivate {--filename=filename | --hosts=hosts}
                            cluster

  Puts hosts into maintenance mode.

  The list of hosts is marked for maintenance, and will be de-prioritized
  from consideration for scheduling.  Note, they are not removed from
  consideration, and may still schedule tasks if resources are very scarce.
  Usually you would mark a larger set of machines for drain, and then do
  them in batches within the larger set, to help drained tasks not land on
  future hosts that will be drained shortly in subsequent batches.
  """
  options = app.get_options()
  HostMaintenance(
      cluster=CLUSTERS[cluster],
      verbosity=options.verbosity,
      bypass_leader_redirect=options.bypass_leader_redirect).start_maintenance(
          parse_hostnames(options.filename, options.hosts))


@app.command
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@requires.exactly('cluster')
def host_activate(cluster):
  """usage: host_activate {--filename=filename | --hosts=hosts}
                          cluster

  Removes maintenance mode from hosts.

  The list of hosts is marked as not in a drained state anymore. This will
  allow normal scheduling to resume on the given list of hosts.
  """
  options = app.get_options()
  HostMaintenance(
      cluster=CLUSTERS[cluster],
      verbosity=options.verbosity,
      bypass_leader_redirect=options.bypass_leader_redirect).end_maintenance(
          parse_hostnames(options.filename, options.hosts))


@app.command
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@app.command_option(POST_DRAIN_SCRIPT_OPTION)
@app.command_option(GROUPING_OPTION)
@app.command_option(OVERRIDE_SLA_PERCENTAGE_OPTION)
@app.command_option(OVERRIDE_SLA_DURATION_OPTION)
@app.command_option(OVERRIDE_SLA_REASON_OPTION)
@app.command_option(UNSAFE_SLA_HOSTS_FILE_OPTION)
@requires.exactly('cluster')
def host_drain(cluster):
  """usage: host_drain {--filename=filename | --hosts=hosts}
                       [--post_drain_script=path]
                       [--grouping=function]
                       [--override_percentage=percentage]
                       [--override_duration=duration]
                       [--override_reason=reason]
                       [--unsafe_hosts_file=unsafe_hosts_filename]
                       cluster

  Asks the scheduler to start maintenance on the list of provided hosts (see host_deactivate
  for more details) and drains any active tasks on them.

  The list of hosts is drained and marked in a drained state.  This will kill
  off any tasks currently running on these hosts, as well as prevent future
  tasks from scheduling on these hosts while they are drained.

  The hosts are left in maintenance mode upon completion. Use host_activate to
  return hosts back to service and allow scheduling tasks on them.
  """
  options = app.get_options()
  drainable_hosts = parse_hostnames(options.filename, options.hosts)
  get_grouping_or_die(options.grouping)

  override_percentage, override_duration = parse_and_validate_sla_overrides(
      options,
      drainable_hosts)

  post_drain_callback = parse_script(options.post_drain_script)

  HostMaintenance(
      cluster=CLUSTERS[cluster],
      verbosity=options.verbosity,
      bypass_leader_redirect=options.bypass_leader_redirect).perform_maintenance(
          drainable_hosts,
          grouping_function=options.grouping,
          percentage=override_percentage,
          duration=override_duration,
          output_file=options.unsafe_hosts_filename,
          callback=post_drain_callback)


@app.command
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@requires.exactly('cluster')
def host_status(cluster):
  """usage: host_status {--filename=filename | --hosts=hosts}
                        cluster

  Print the drain status of each supplied host.
  """
  options = app.get_options()
  checkable_hosts = parse_hostnames(options.filename, options.hosts)
  statuses = HostMaintenance(
      cluster=CLUSTERS[cluster],
      verbosity=options.verbosity,
      bypass_leader_redirect=options.bypass_leader_redirect).check_status(checkable_hosts)

  for pair in statuses:
    log.info("%s is in state: %s" % pair)
