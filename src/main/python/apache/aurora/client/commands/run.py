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

from apache.aurora.client.base import die
from apache.aurora.client.options import (
    EXECUTOR_SANDBOX_OPTION,
    SSH_USER_OPTION,
)
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.clusters import CLUSTERS
from apache.aurora.client.api.command_runner import DistributedCommandRunner

from twitter.common import app


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
  dcr = DistributedCommandRunner(cluster, role, env, [name], options.ssh_user)
  dcr.run(command, parallelism=options.num_threads, executor_sandbox=options.executor_sandbox)
