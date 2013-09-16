from twitter.common import app
from twitter.aurora.client.base import die
from twitter.aurora.client.options import (
    EXECUTOR_SANDBOX_OPTION,
    SSH_USER_OPTION,
)
from twitter.aurora.common.aurora_job_key import AuroraJobKey
from twitter.aurora.common.clusters import CLUSTERS
from twitter.aurora.client.api.command_runner import DistributedCommandRunner


@app.command
@app.command_option('-t', '--threads', type=int, default=1, dest='num_threads',
    help='The number of threads to use.')
@app.command_option(SSH_USER_OPTION)
@app.command_option(EXECUTOR_SANDBOX_OPTION)
def run(args, options):
  """usage: run cluster/role/env/job cmd

  Runs a shell command on all machines currently hosting shards of a single job.

  This feature supports the same command line wildcards that are used to
  populate a job's commands.

  This means anything in the {{mesos.*}} and {{thermos.*}} namespaces.
  """
  # TODO(William Farner): Add support for invoking on individual shards.
  # TODO(Kevin Sweeney): Restore the ability to run across jobs with globs (See MESOS-3010).
  if not args:
    die('job path is required')
  job_path = args.pop(0)
  try:
    cluster_name, role, env, name = AuroraJobKey.from_path(job_path)
  except AuroraJobKey.Error as e:
    die('Invalid job path "%s": %s' % (job_path, e))

  command = ' '.join(args)
  cluster = CLUSTERS[cluster_name]
  dcr = DistributedCommandRunner(cluster, role, [name], options.ssh_user)
  dcr.run(command, parallelism=options.num_threads, executor_sandbox=options.executor_sandbox)
