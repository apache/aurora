#
# Copyright 2013 Apache Software Foundation
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

import optparse
import os
import subprocess

from twitter.common import app, log

from apache.aurora.admin.host_maintenance import HostMaintenance
from apache.aurora.client.base import die, FILENAME_OPTION, HOSTS_OPTION, parse_hosts, requires
from apache.aurora.common.clusters import CLUSTERS

GROUPING_OPTION = optparse.Option(
    '--grouping',
    type='choice',
    choices=HostMaintenance.GROUPING_FUNCTIONS.keys(),
    metavar='GROUPING',
    default=HostMaintenance.DEFAULT_GROUPING,
    dest='grouping',
    help='Grouping function to use to group hosts.  Options: %s.  Default: %%default' % (
        ', '.join(HostMaintenance.GROUPING_FUNCTIONS.keys())))


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
      parse_hosts(options.filename, options.hosts))


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
      parse_hosts(options.filename, options.hosts))


@app.command
@app.command_option('--groups_per_batch', dest='groups_per_batch', default=1,
    help='Number of groups to operate on at a time.')
@app.command_option('--post_drain_script', dest='post_drain_script', default=None,
    help='Path to a script to run for each host.')
@app.command_option(FILENAME_OPTION)
@app.command_option(HOSTS_OPTION)
@app.command_option(GROUPING_OPTION)
@requires.exactly('cluster')
def perform_maintenance_hosts(cluster):
  """usage: perform_maintenance_hosts {--filename=filename | --hosts=hosts}
                                      [--groups_per_batch=num]
                                      [--post_drain_script=path]
                                      [--grouping=function]
                                      cluster

  Asks the scheduler to remove any running tasks from the machine and remove it
  from service temporarily, perform some action on them, then return the machines
  to service.
  """
  options = app.get_options()
  drainable_hosts = parse_hosts(options.filename, options.hosts)

  if options.post_drain_script:
    if not os.path.exists(options.post_drain_script):
      die("No such file: %s" % options.post_drain_script)
    cmd = os.path.abspath(options.post_drain_script)
    drained_callback = lambda host: subprocess.Popen([cmd, host])
  else:
    drained_callback = None

  HostMaintenance(CLUSTERS[cluster], options.verbosity).perform_maintenance(
      drainable_hosts,
      groups_per_batch=int(options.groups_per_batch),
      callback=drained_callback,
      grouping_function=options.grouping)


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
  checkable_hosts = parse_hosts(options.filename, options.hosts)
  statuses = HostMaintenance(CLUSTERS[cluster], options.verbosity).check_status(checkable_hosts)
  for pair in statuses:
    log.info("%s is in state: %s" % pair)
