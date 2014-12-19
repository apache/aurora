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

import subprocess

from twitter.common import app

from apache.aurora.client.api.command_runner import DistributedCommandRunner
from apache.aurora.client.base import AURORA_V1_USER_AGENT_NAME, check_and_log_response, die
from apache.aurora.client.factory import make_client
from apache.aurora.client.options import EXECUTOR_SANDBOX_OPTION, SSH_USER_OPTION
from apache.aurora.common.aurora_job_key import AuroraJobKey


# duplicate of same function in core.py; since this is a temporary deprecation warning,
# we don't want to add new source files or inter-source dependencies. Duplicating this
# two-line function is the lesser evil.
def v1_deprecation_warning(old, new):
  print("WARNING: %s is an aurora clientv1 command which will be deprecated soon" % old)
  print("To run this command using clientv2, use 'aurora %s'" % " ".join(new))


@app.command
@app.command_option(EXECUTOR_SANDBOX_OPTION)
@app.command_option(SSH_USER_OPTION)
@app.command_option('-L', dest='tunnels', action='append', metavar='PORT:NAME',
                    default=[],
                    help="Add tunnel from local port PORT to remote named port NAME.")
def ssh(args, options):
  """usage: ssh cluster/role/env/job shard [args...]

  Initiate an SSH session on the machine that a shard is running on.
  """
  if not args:
    die('Job path is required')
  job_path = args.pop(0)
  try:
    cluster_name, role, env, name = AuroraJobKey.from_path(job_path)
  except AuroraJobKey.Error as e:
    die('Invalid job path "%s": %s' % (job_path, e))
  if not args:
    die('Shard is required')
  try:
    shard = int(args.pop(0))
  except ValueError:
    die('Shard must be an integer')

  newcmd = ["task", "ssh", "%s/%s" % (job_path, shard)]
  if len(options.tunnels) > 0:
    for t in options.tunnels:
      newcmd.append("--tunnels=%s" % t)
  if options.ssh_user is not None:
    newcmd.append("--ssh-user=%s" % options.ssh_user)
  if options.executor_sandbox:
    newcmd.append("--executor-sandbox")
  if len(args) > 0:
    newcmd.append("--command=\"%s\"" % " ".join(args))
  v1_deprecation_warning("ssh", newcmd)

  api = make_client(cluster_name, AURORA_V1_USER_AGENT_NAME)
  resp = api.query(api.build_query(role, name, set([int(shard)]), env=env))
  check_and_log_response(resp)

  if (resp.result.scheduleStatusResult.tasks is None or
      len(resp.result.scheduleStatusResult.tasks) == 0):
    die("Job %s not found" % job_path)
  first_task = resp.result.scheduleStatusResult.tasks[0]
  remote_cmd = 'bash' if not args else ' '.join(args)
  command = DistributedCommandRunner.substitute(remote_cmd, first_task,
      api.cluster, executor_sandbox=options.executor_sandbox)

  ssh_command = ['ssh', '-t']

  assigned = first_task.assignedTask
  role = assigned.task.job.role if assigned.task.job else assigned.task.owner.role
  slave_host = assigned.slaveHost

  for tunnel in options.tunnels:
    try:
      port, name = tunnel.split(':')
      port = int(port)
    except ValueError:
      die('Could not parse tunnel: %s.  Must be of form PORT:NAME' % tunnel)
    if name not in assigned.assignedPorts:
      die('Task %s has no port named %s' % (assigned.taskId, name))
    ssh_command += [
        '-L', '%d:%s:%d' % (port, slave_host, assigned.assignedPorts[name])]

  ssh_command += ['%s@%s' % (options.ssh_user or role, slave_host), command]
  return subprocess.call(ssh_command)
