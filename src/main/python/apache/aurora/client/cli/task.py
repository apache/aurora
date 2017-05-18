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

import os
import subprocess
from argparse import ArgumentTypeError

from pystachio import Environment, String

from apache.aurora.client.api.command_runner import (
    DistributedCommandRunner,
    InstanceDistributedCommandRunner
)
from apache.aurora.client.base import combine_messages
from apache.aurora.client.cli import EXIT_INVALID_PARAMETER, EXIT_OK, Noun, Verb
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    ALL_INSTANCES,
    EXECUTOR_SANDBOX_OPTION,
    INSTANCES_SPEC_ARGUMENT,
    SCP_DEST_ARGUMENT,
    SCP_OPTIONS,
    SCP_SOURCE_ARGUMENT,
    SSH_INSTANCE_ARGUMENT,
    SSH_OPTIONS,
    SSH_USER_OPTION,
    CommandOption,
    parse_task_instance_key
)
from apache.aurora.common.clusters import CLUSTERS
from apache.thermos.config.schema import ThermosContext


class RunCommand(Verb):
  @property
  def name(self):
    return 'run'

  @property
  def help(self):
    return """runs a shell command on machines currently hosting instances of a single job.
  This feature supports the same command line wildcards that are used to
  populate a job's commands.
  This means anything in the {{mesos.*}} and {{thermos.*}} namespaces.
  """

  def get_options(self):
    return [
        CommandOption('--threads', '-t', type=int, default=1, dest='num_threads',
            help='Number of threads to use'),
        SSH_USER_OPTION,
        SSH_OPTIONS,
        EXECUTOR_SANDBOX_OPTION,
        INSTANCES_SPEC_ARGUMENT,
        CommandOption('cmd', type=str, metavar="unix_command_line")
    ]

  def execute(self, context):
    (cluster_name, role, env, name), instances = context.options.instance_spec
    cluster = CLUSTERS[cluster_name]
    dcr = InstanceDistributedCommandRunner(
        cluster,
        role,
        env,
        name,
        context.options.ssh_user,
        context.options.ssh_options,
        instances)
    dcr.run(context.options.cmd, parallelism=context.options.num_threads,
        executor_sandbox=context.options.executor_sandbox)
    return EXIT_OK


class SshCommand(Verb):
  @property
  def name(self):
    return 'ssh'

  @property
  def help(self):
    return """initiates an SSH session on the machine that a task instance is running on.
  """

  def get_options(self):
    return [
        SSH_INSTANCE_ARGUMENT,
        SSH_USER_OPTION,
        SSH_OPTIONS,
        EXECUTOR_SANDBOX_OPTION,
        CommandOption('--tunnels', '-L', dest='tunnels', action='append', metavar='PORT:NAME',
            default=[],
            help="Add tunnel from local port PART to remote named port NAME"),
        CommandOption('--command', '-c', dest='command', type=str, default=None,
            metavar="unix_command_line",
            help="Command to execute through the ssh connection."),
    ]

  def execute(self, context):
    (cluster, role, env, name) = context.options.instance_spec.jobkey
    instance = (None if context.options.instance_spec.instance == ALL_INSTANCES else
        set(context.options.instance_spec.instance))
    if instance is None and context.options.command:
      raise context.CommandError(EXIT_INVALID_PARAMETER,
          'INSTANCE must be specified when --command option is given')
    api = context.get_api(cluster)
    resp = api.query(api.build_query(role, name, env=env, instances=instance))
    context.log_response_and_raise(resp,
        err_msg=('Unable to get information about instance: %s' % combine_messages(resp)))
    if (resp.result.scheduleStatusResult.tasks is None or
        len(resp.result.scheduleStatusResult.tasks) == 0):
      raise context.CommandError(EXIT_INVALID_PARAMETER,
          "Job %s not found" % context.options.instance_spec.jobkey)
    first_task = resp.result.scheduleStatusResult.tasks[0]
    remote_cmd = context.options.command or 'bash'
    command = DistributedCommandRunner.substitute(
        remote_cmd,
        first_task,
        api.cluster,
        executor_sandbox=context.options.executor_sandbox)

    ssh_command = ['ssh', '-t']
    ssh_command += context.options.ssh_options if context.options.ssh_options else []
    assigned = first_task.assignedTask
    role = assigned.task.job.role
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


class ScpCommand(Verb):

  JOB_NOT_FOUND_ERROR_MSG = 'Job or instance %s/%s not found'
  TILDE_USAGE_ERROR_MSG = 'Command does not support tilde expansion for path: %s'

  @property
  def name(self):
    return 'scp'

  @property
  def help(self):
    return """executes an scp to/from/between task instance(s). The task sandbox acts as
  the relative root.
  """

  @staticmethod
  def _extract_task_instance_and_path(context, file_path):
    key = file_path.split(':', 1)
    try:
      if (len(key) == 1):
        return (None, key[0])  # No jobkey specified
      return (parse_task_instance_key(key[0]), key[1])
    except ArgumentTypeError as e:
      raise context.CommandError(EXIT_INVALID_PARAMETER, str(e))

  @staticmethod
  def _build_path(context, target):
    (task_instance, path) = ScpCommand._extract_task_instance_and_path(context, target)

    # No jobkey is specified therefore we are using a local path.
    if (task_instance is None):
      return path

    # Jobkey specified, we want to convert to the user@host:file scp format
    (cluster, role, env, name) = task_instance.jobkey
    instance = set([task_instance.instance])
    api = context.get_api(cluster)
    resp = api.query(api.build_query(role, name, env=env, instances=instance))
    context.log_response_and_raise(resp,
        err_msg=('Unable to get information about instance: %s' % combine_messages(resp)))
    if (resp.result.scheduleStatusResult.tasks is None or
        len(resp.result.scheduleStatusResult.tasks) == 0):
      raise context.CommandError(EXIT_INVALID_PARAMETER,
          ScpCommand.JOB_NOT_FOUND_ERROR_MSG % (task_instance.jobkey, task_instance.instance))
    first_task = resp.result.scheduleStatusResult.tasks[0]
    assigned = first_task.assignedTask
    role = assigned.task.job.role
    slave_host = assigned.slaveHost

    # If path is absolute, use that. Else if it is a tilde expansion, throw an error.
    # Otherwise, use sandbox as relative root.
    normalized_input_path = os.path.normpath(path)
    if (os.path.isabs(normalized_input_path)):
      final_path = normalized_input_path
    elif (normalized_input_path.startswith('~/') or normalized_input_path == '~'):
      raise context.CommandError(EXIT_INVALID_PARAMETER, ScpCommand.TILDE_USAGE_ERROR_MSG % path)
    else:
      sandbox_path_pre_format = DistributedCommandRunner.thermos_sandbox(
          api.cluster,
          executor_sandbox=context.options.executor_sandbox)
      thermos_namespace = ThermosContext(
          task_id=assigned.taskId,
          ports=assigned.assignedPorts)
      sandbox_path = String(sandbox_path_pre_format) % Environment(thermos=thermos_namespace)
      # Join the individual folders to the sandbox path to build safely
      final_path = os.path.join(str(sandbox_path), *normalized_input_path.split(os.sep))

    return '%s@%s:%s' % (role, slave_host, final_path)

  def get_options(self):
    return [
        SCP_SOURCE_ARGUMENT,
        SCP_DEST_ARGUMENT,
        SCP_OPTIONS,
        EXECUTOR_SANDBOX_OPTION
    ]

  def execute(self, context):
    scp_command = ['scp']
    scp_command += context.options.scp_options if context.options.scp_options else []
    scp_command += [ScpCommand._build_path(context, p) for p in context.options.source]
    scp_command += [ScpCommand._build_path(context, context.options.dest)]
    return subprocess.call(scp_command)


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
    self.register_verb(ScpCommand())
