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

"""Command-line entry point to the Thermos Executor

This module wraps the Thermos Executor into an executable suitable for launching by a Mesos
slave.

"""

import os

from twitter.common import app, log
from twitter.common.log.options import LogOptions

from apache.aurora.executor.aurora_executor import AuroraExecutor
from apache.aurora.executor.common.announcer import DefaultAnnouncerCheckerProvider
from apache.aurora.executor.common.executor_timeout import ExecutorTimeout
from apache.aurora.executor.common.health_checker import HealthCheckerProvider
from apache.aurora.executor.common.resource_manager import ResourceManagerProvider
from apache.aurora.executor.common.sandbox import DefaultSandboxProvider
from apache.aurora.executor.thermos_task_runner import (
    DefaultThermosTaskRunnerProvider,
    UserOverrideThermosTaskRunnerProvider
)
from apache.thermos.common.constants import DEFAULT_CHECKPOINT_ROOT

try:
  from mesos.native import MesosExecutorDriver
except ImportError:
  MesosExecutorDriver = None


CWD = os.environ.get('MESOS_SANDBOX', '.')

app.configure(debug=True)
LogOptions.set_simple(True)
LogOptions.set_disk_log_level('DEBUG')
LogOptions.set_log_dir(CWD)


app.add_option(
    '--announcer-enable',
    dest='announcer_enable',
    action='store_true',
    default=False,
    help='Enable the ServerSet announcer for this executor.  Jobs must still activate using '
         'the Announcer configuration.')


app.add_option(
    '--announcer-ensemble',
    dest='announcer_ensemble',
    type=str,
    default=None,
    help='The ensemble to which the Announcer should register ServerSets.')


app.add_option(
    '--announcer-serverset-path',
    dest='announcer_serverset_path',
    type=str,
    default='/aurora',
    help='The root of the tree into which ServerSets should be announced.  The paths will '
         'be of the form $ROOT/$ROLE/$ENVIRONMENT/$JOBNAME.')


app.add_option(
    '--execute-as-user',
    dest='execute_as_user',
    type=str,
    help='Causes the executor to override the user specified by aurora.')


app.add_option(
    '--checkpoint-root',
    dest='checkpoint_root',
    metavar='PATH',
    default=DEFAULT_CHECKPOINT_ROOT,
    help='The checkpoint root where Thermos task checkpoints are stored.')


app.add_option(
    '--nosetuid',
    dest='nosetuid',
    action='store_true',
    help='If set, the executor will not attempt to change users when running thermos_runner',
    default=False)


# TODO(wickman) Consider just having the OSS version require pip installed
# thermos_runner binaries on every machine and instead of embedding the pex
# as a resource, shell out to one on the PATH.
def dump_runner_pex():
  import pkg_resources
  import apache.aurora.executor.resources
  pex_name = 'thermos_runner.pex'
  runner_pex = os.path.join(os.path.abspath(CWD), pex_name)
  with open(runner_pex, 'w') as fp:
    # TODO(wickman) Use shutil.copyfileobj to reduce memory footprint here.
    fp.write(pkg_resources.resource_stream(
        apache.aurora.executor.resources.__name__, pex_name).read())
  return runner_pex


class UserOverrideDirectorySandboxProvider(DefaultSandboxProvider):
  def __init__(self, user_override):
    self._user_override = user_override

  def _get_sandbox_user(self, assigned_task):
    return self._user_override


def proxy_main():
  def main(args, options):
    if MesosExecutorDriver is None:
      app.error('Could not load MesosExecutorDriver!')

    # status providers:
    status_providers = [
        HealthCheckerProvider(),
        ResourceManagerProvider(checkpoint_root=options.checkpoint_root)
    ]

    if options.announcer_enable:
      if options.announcer_ensemble is None:
        app.error('Must specify --announcer-ensemble if the announcer is enabled.')
      status_providers.append(DefaultAnnouncerCheckerProvider(
        options.announcer_ensemble, options.announcer_serverset_path))

    # Create executor stub
    if options.execute_as_user or options.nosetuid:
      # If nosetuid is set, execute_as_user is also None
      thermos_runner_provider = UserOverrideThermosTaskRunnerProvider(
        dump_runner_pex(),
        artifact_dir=os.path.abspath(CWD)
      )
      thermos_runner_provider.set_role(None)

      thermos_executor = AuroraExecutor(
        runner_provider=thermos_runner_provider,
        status_providers=status_providers,
        sandbox_provider=UserOverrideDirectorySandboxProvider(options.execute_as_user)
      )
    else:
      thermos_runner_provider = DefaultThermosTaskRunnerProvider(
        dump_runner_pex(),
        artifact_dir=os.path.abspath(CWD)
      )

      thermos_executor = AuroraExecutor(
        runner_provider=thermos_runner_provider,
        status_providers=status_providers
      )

    # Create driver stub
    driver = MesosExecutorDriver(thermos_executor)

    # This is an ephemeral executor -- shutdown if we receive no tasks within a certain
    # time period
    ExecutorTimeout(thermos_executor.launched, driver).start()

    # Start executor
    driver.run()

    log.info('MesosExecutorDriver.run() has finished.')

  app.main()
