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

"""Implementation of the Task noun for the Apache Aurora client.
"""

from __future__ import print_function
from datetime import datetime
import json
import os
import pprint
import subprocess
import sys
import time

from apache.aurora.client.api.command_runner import DistributedCommandRunner
from apache.aurora.client.api.updater_util import UpdaterConfig
from apache.aurora.client.cli import (
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_CONFIGURATION,
    EXIT_INVALID_PARAMETER,
    EXIT_OK,
    Noun,
    Verb,
)
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    BATCH_OPTION,
    BIND_OPTION,
    BROWSER_OPTION,
    CommandOption,
    CONFIG_ARGUMENT,
    EXECUTOR_SANDBOX_OPTION,
    FORCE_OPTION,
    HEALTHCHECK_OPTION,
    JOBSPEC_ARGUMENT,
    JSON_READ_OPTION,
    JSON_WRITE_OPTION,
    SSH_USER_OPTION,
    TASK_INSTANCE_ARGUMENT,
    WATCH_OPTION,
)
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.clusters import CLUSTERS

from gen.apache.aurora.api.constants import ACTIVE_STATES, AURORA_EXECUTOR_NAME
from gen.apache.aurora.api.ttypes import (
    ExecutorConfig,
    ResponseCode,
    ScheduleStatus,
)

from pystachio.config import Config
from thrift.TSerialization import serialize
from thrift.protocol import TJSONProtocol



class RunCommand(Verb):
  @property
  def name(self):
    return 'run'

  @property
  def help(self):
    return """Usage: aurora task run cluster/role/env/job cmd

  Runs a shell command on all machines currently hosting instances of a single job.

  This feature supports the same command line wildcards that are used to
  populate a job's commands.

  This means anything in the {{mesos.*}} and {{thermos.*}} namespaces.
  """

  def get_options(self):
    return [
        CommandOption('--threads', '-t', type=int, default=1, dest='num_threads',
            help='Number of threads to use'),
        SSH_USER_OPTION,
        EXECUTOR_SANDBOX_OPTION,
        JOBSPEC_ARGUMENT,
        CommandOption('cmd', type=str)
    ]

  def execute(self, context):
    # TODO(mchucarroll): add options to specify which instances to run on (AURORA-198)
    cluster_name, role, env, name = context.options.jobspec
    cluster = CLUSTERS[cluster_name]
    dcr = DistributedCommandRunner(cluster, role, env, [name], context.options.ssh_user)
    dcr.run(context.options.cmd, parallelism=context.options.num_threads,
        executor_sandbox=context.options.executor_sandbox)


class SshCommand(Verb):
  @property
  def name(self):
    return 'ssh'

  @property
  def help(self):
    return """usage: aurora task ssh cluster/role/env/job/instance [args...]

  Initiate an SSH session on the machine that a task instance is running on.
  """

  def get_options(self):
    return [
        SSH_USER_OPTION,
        EXECUTOR_SANDBOX_OPTION,
        CommandOption('--tunnels', '-L', dest='tunnels', action='append', metavar='PORT:NAME',
            default=[],
            help="Add tunnel from local port PART to remote named port NAME"),
        CommandOption('--command', '-c', dest='command', type=str, default=None,
            help="Command to execute through the ssh connection."),
        TASK_INSTANCE_ARGUMENT
    ]

  def execute(self, context):
    (cluster, role, env, name) = context.options.task_instance.jobkey
    instance = context.options.task_instance.instance

    api = context.get_api(cluster)
    resp = api.query(api.build_query(role, name, set([int(instance)]), env=env))
    if resp.responseCode != ResponseCode.OK:
      raise context.CommandError('Unable to get information about instance: %s' % resp.message)
    first_task = resp.result.scheduleStatusResult.tasks[0]
    remote_cmd = context.options.command or 'bash'
    command = DistributedCommandRunner.substitute(remote_cmd, first_task,
        api.cluster, executor_sandbox=context.options.executor_sandbox)

    ssh_command = ['ssh', '-t']
    role = first_task.assignedTask.task.owner.role
    slave_host = first_task.assignedTask.slaveHost

    for tunnel in context.options.tunnels:
      try:
        port, name = tunnel.split(':')
        port = int(port)
      except ValueError:
        die('Could not parse tunnel: %s.  Must be of form PORT:NAME' % tunnel)
      if name not in first_task.assignedTask.assignedPorts:
        die('Task %s has no port named %s' % (first_task.assignedTask.taskId, name))
      ssh_command += [
          '-L', '%d:%s:%d' % (port, slave_host, first_task.assignedTask.assignedPorts[name])]

    ssh_command += ['%s@%s' % (context.options.ssh_user or role, slave_host), command]
    return subprocess.call(ssh_command)


class Task(Noun):
  @property
  def name(self):
    return 'task'

  @property
  def help(self):
    return "Work with a task running in an Apache Aurora cluster"

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(Task, self).__init__()
    self.register_verb(RunCommand())
    self.register_verb(SshCommand())
