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

import subprocess

from apache.aurora.client.api.command_runner import (
    DistributedCommandRunner,
    InstanceDistributedCommandRunner
)
from apache.aurora.client.base import combine_messages
from apache.aurora.client.cli import EXIT_INVALID_PARAMETER, EXIT_OK, Noun, Verb
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    CommandOption,
    EXECUTOR_SANDBOX_OPTION,
    INSTANCES_SPEC_ARGUMENT,
    SSH_USER_OPTION,
    TASK_INSTANCE_ARGUMENT
)
from apache.aurora.common.clusters import CLUSTERS


class RunCommand(Verb):
  @property
  def name(self):
    return 'run'

  @property
  def help(self):
    return """Usage: aurora task run cluster/role/env/job cmd

  Runs a shell command on machines currently hosting instances of a single job.

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
        INSTANCES_SPEC_ARGUMENT,
        CommandOption('cmd', type=str, metavar="unix_command_line")
    ]

  def execute(self, context):
    (cluster_name, role, env, name), instances = context.options.instance_spec
    cluster = CLUSTERS[cluster_name]
    dcr = InstanceDistributedCommandRunner(cluster, role, env, name,
        context.options.ssh_user, instances)
    dcr.run(context.options.cmd, parallelism=context.options.num_threads,
        executor_sandbox=context.options.executor_sandbox)
    return EXIT_OK


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
            metavar="unix_command_line",
            help="Command to execute through the ssh connection."),
        TASK_INSTANCE_ARGUMENT
    ]

  def execute(self, context):
    (cluster, role, env, name) = context.options.task_instance.jobkey
    instance = context.options.task_instance.instance

    api = context.get_api(cluster)
    resp = api.query(api.build_query(role, name, set([int(instance)]), env=env))
    context.check_and_log_response(resp,
        err_msg=('Unable to get information about instance: %s' % combine_messages(resp)))
    if (resp.result.scheduleStatusResult.tasks is None or
        len(resp.result.scheduleStatusResult.tasks) == 0):
      raise context.CommandError(EXIT_INVALID_PARAMETER,
          "Job %s not found" % context.options.task_instance.jobkey)
    first_task = resp.result.scheduleStatusResult.tasks[0]
    remote_cmd = context.options.command or 'bash'
    command = DistributedCommandRunner.substitute(remote_cmd, first_task,
        api.cluster, executor_sandbox=context.options.executor_sandbox)

    ssh_command = ['ssh', '-t']
    assigned = first_task.assignedTask
    role = assigned.task.job.role if assigned.task.job else assigned.task.owner.role
    slave_host = assigned.slaveHost

    for tunnel in context.options.tunnels:
      try:
        port, name = tunnel.split(':')
        port = int(port)
      except ValueError:
        raise context.CommandError(EXIT_INVALID_PARAMETER,
            'Could not parse tunnel: %s.  Must be of form PORT:NAME' % tunnel)
      if name not in assigned.assignedPorts:
        raise context.CommandError(EXIT_INVALID_PARAMETER,
            'Task %s has no port named %s' % (assigned.taskId, name))
      ssh_command += [
          '-L', '%d:%s:%d' % (port, slave_host, assigned.assignedPorts[name])]

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
