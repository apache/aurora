import subprocess

from twitter.common import app
from twitter.aurora.client.base import check_and_log_response, die
from twitter.aurora.client.factory import make_client
from twitter.aurora.client.options import (
    EXECUTOR_SANDBOX_OPTION,
    SSH_USER_OPTION,
)
from twitter.aurora.common.aurora_job_key import AuroraJobKey
from twitter.aurora.client.api.command_runner import DistributedCommandRunner


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
  api = make_client(cluster_name)
  resp = api.query(api.build_query(role, name, set([int(shard)]), env=env))
  check_and_log_response(resp)

  first_task = resp.result.scheduleStatusResult.tasks[0]
  remote_cmd = 'bash' if not args else ' '.join(args)
  command = DistributedCommandRunner.substitute(remote_cmd, first_task,
      api.cluster, executor_sandbox=options.executor_sandbox)

  ssh_command = ['ssh', '-t']

  role = first_task.assignedTask.task.owner.role
  slave_host = first_task.assignedTask.slaveHost

  for tunnel in options.tunnels:
    try:
      port, name = tunnel.split(':')
      port = int(port)
    except ValueError:
      die('Could not parse tunnel: %s.  Must be of form PORT:NAME' % tunnel)
    if name not in first_task.assignedTask.assignedPorts:
      die('Task %s has no port named %s' % (first_task.assignedTask.taskId, name))
    ssh_command += [
        '-L', '%d:%s:%d' % (port, slave_host, first_task.assignedTask.assignedPorts[name])]

  ssh_command += ['%s@%s' % (options.ssh_user or role, slave_host), command]
  return subprocess.call(ssh_command)
