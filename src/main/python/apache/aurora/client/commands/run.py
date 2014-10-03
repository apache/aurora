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

from twitter.common import app

from apache.aurora.client.api.command_runner import DistributedCommandRunner
from apache.aurora.client.base import die
from apache.aurora.client.options import EXECUTOR_SANDBOX_OPTION, SSH_USER_OPTION
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.clusters import CLUSTERS


# duplicate of same function in core.py; since this is a temporary deprecation warning,
# we don't want to add new source files or inter-source dependencies. Duplicating this
# two-line function is the lesser evil.
def v1_deprecation_warning(old, new):
  print("WARNING: %s is an aurora clientv1 command which will be deprecated soon" % old)
  print("To run this command using clientv2, use 'aurora %s'" % " ".join(new))


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
  new_cmd = ["task", "run"]
  if options.num_threads != 1:
    new_cmd.append("--threads=%s" % options.num_threads)
  if options.ssh_user is not None:
    new_cmd.append("--ssh-user=%s" % options.ssh_user)
  if options.executor_sandbox:
    new_cmd.append("--executor-sandbox")
  new_cmd.append("\"%s\"" % " ".join(args))
  v1_deprecation_warning("ssh", new_cmd)

  try:
    cluster_name, role, env, name = AuroraJobKey.from_path(job_path)
  except AuroraJobKey.Error as e:
    die('Invalid job path "%s": %s' % (job_path, e))

  command = ' '.join(args)
  cluster = CLUSTERS[cluster_name]
  dcr = DistributedCommandRunner(cluster, role, env, [name], options.ssh_user)
  dcr.run(command, parallelism=options.num_threads, executor_sandbox=options.executor_sandbox)
