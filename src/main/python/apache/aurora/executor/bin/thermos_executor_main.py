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
agent.

"""

from __future__ import print_function

import os
import sys
import traceback

from twitter.common import app, log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.log.options import LogOptions

from apache.aurora.config.schema.base import LoggerDestination, LoggerMode
from apache.aurora.executor.aurora_executor import AuroraExecutor
from apache.aurora.executor.common.announcer import DefaultAnnouncerCheckerProvider, make_zk_auth
from apache.aurora.executor.common.executor_timeout import ExecutorTimeout
from apache.aurora.executor.common.health_checker import HealthCheckerProvider
from apache.aurora.executor.common.path_detector import MesosPathDetector
from apache.aurora.executor.common.resource_manager import ResourceManagerProvider
from apache.aurora.executor.common.sandbox import DefaultSandboxProvider
from apache.aurora.executor.thermos_task_runner import (
    DefaultThermosTaskRunnerProvider,
    UserOverrideThermosTaskRunnerProvider
)
from apache.thermos.common.excepthook import ExceptionTerminationHandler

try:
  from mesos.executor import MesosExecutorDriver
except ImportError:
  print(traceback.format_exc(), file=sys.stderr)
  MesosExecutorDriver = None


CWD = os.environ.get('MESOS_SANDBOX', '.')

app.configure(debug=True)
LogOptions.set_simple(True)
LogOptions.set_disk_log_level('DEBUG')
LogOptions.set_log_dir(CWD)


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
    '--announcer-allow-custom-serverset-path',
    dest='announcer_allow_custom_serverset_path',
    action='store_true',
    default=False,
    help='Allows setting arbitrary serverset path through the Announcer configuration.')

app.add_option(
    '--announcer-hostname',
    dest='announcer_hostname',
    type=str,
    default=None,
    help='Set hostname to be announced. By default it is'
         'the --hostname argument passed into the Mesos agent.')

app.add_option(
    '--announcer-zookeeper-auth-config',
    dest='announcer_zookeeper_auth_config',
    type=str,
    default=None,
    help='Path to ZooKeeper authentication to use for announcer nodes.')

app.add_option(
    '--mesos-containerizer-path',
    dest='mesos_containerizer_path',
    type=str,
    help='The path to the mesos-containerizer executable that will be used to isolate the task''s '
         'filesystem when using a filesystem image. Note: this path should match the value of the '
         'Mesos Agent''s -launcher_dir flag.',
    default='/usr/libexec/mesos/mesos-containerizer')


app.add_option(
    '--no-create-user',
    dest='no_create_user',
    action='store_true',
    help='If set, the executor will not attempt to create the task''s user/group under the '
         'filesystem image (only applicable when launching a task with a filesystem image).',
    default=False)


# Ideally we'd just be able to use the value of the MESOS_SANDBOX environment variable to get this
# directly from Mesos. Unfortunately, our method of isolating the task's filesystem does not involve
# setting a ContainerInfo on the task, but instead mounts the task's filesystem as a Volume with an
# Image set. In practice this means the value of MESOS_SANDBOX matches the value of the
# MESOS_DIRECTORY environment variable.
app.add_option(
    '--sandbox-mount-point',
    dest='sandbox_mount_point',
    type=str,
    help='The path under the task''s filesystem where the sandbox directory should be mounted '
         '(only applicable when launching a task with a filesystem image). Note: for '
         'consistency, this path should match the value of the Mesos Agent''s '
         '-sandbox_directory flag.',
    default='/mnt/mesos/sandbox')


app.add_option(
    '--execute-as-user',
    dest='execute_as_user',
    type=str,
    help='Causes the executor to override the user specified by aurora.')


app.add_option(
    '--nosetuid',
    dest='nosetuid',
    action='store_true',
    help='If set, the executor will not attempt to change users when running thermos_runner',
    default=False)


app.add_option(
    '--nosetuid-health-checks',
    dest='nosetuid_health_checks',
    action="store_true",
    help='If set, the executor will not run shell health checks as job\'s role\'s user',
    default=False)


app.add_option(
    '--runner-logger-destination',
    dest='runner_logger_destination',
    choices=LoggerDestination.VALUES,
    help='The logger destination %r to use for all processes run by thermos.'
      % (LoggerDestination.VALUES,))


app.add_option(
    '--runner-logger-mode',
    dest='runner_logger_mode',
    choices=LoggerMode.VALUES,
    help='The logger mode %r to use for all processes run by thermos.' % (LoggerMode.VALUES,))


app.add_option(
    '--runner-rotate-log-size-mb',
    dest='runner_rotate_log_size_mb',
    type=int,
    help='Maximum size of the rotated stdout/stderr logs emitted by the thermos runner in MiB.')


app.add_option(
    '--runner-rotate-log-backups',
    dest='runner_rotate_log_backups',
    type=int,
    help='Maximum number of rotated stdout/stderr logs emitted by the thermos runner.')


app.add_option(
     "--preserve_env",
     dest="preserve_env",
     default=False,
     action='store_true',
     help="Preserve thermos runners' environment variables for the task being run.")

app.add_option(
    '--stop_timeout_in_secs',
    dest='stop_timeout_in_secs',
    type=int,
    default=120,
    help='The maximum amount of time to wait (in seconds) when gracefully killing a task before '
         'beginning forceful termination. Graceful and forceful termination is defined in '
         'HttpLifecycleConfig (see Task Lifecycle documentation for more info on termination).')


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


def initialize(options):
  cwd_path = os.path.abspath(CWD)
  checkpoint_root = os.path.join(cwd_path, MesosPathDetector.DEFAULT_SANDBOX_PATH)

  # status providers:
  status_providers = [
      HealthCheckerProvider(
          nosetuid_health_checks=options.nosetuid_health_checks,
          mesos_containerizer_path=options.mesos_containerizer_path),
      ResourceManagerProvider(checkpoint_root=checkpoint_root)
  ]

  if options.announcer_ensemble is not None:
    status_providers.append(DefaultAnnouncerCheckerProvider(
      options.announcer_ensemble,
      options.announcer_serverset_path,
      options.announcer_allow_custom_serverset_path,
      options.announcer_hostname,
      make_zk_auth(options.announcer_zookeeper_auth_config)
    ))

  # Create executor stub
  if options.execute_as_user or options.nosetuid:
    # If nosetuid is set, execute_as_user is also None
    thermos_runner_provider = UserOverrideThermosTaskRunnerProvider(
      dump_runner_pex(),
      checkpoint_root,
      artifact_dir=cwd_path,
      process_logger_destination=options.runner_logger_destination,
      process_logger_mode=options.runner_logger_mode,
      rotate_log_size_mb=options.runner_rotate_log_size_mb,
      rotate_log_backups=options.runner_rotate_log_backups,
      preserve_env=options.preserve_env,
      mesos_containerizer_path=options.mesos_containerizer_path
    )
    thermos_runner_provider.set_role(None)

    thermos_executor = AuroraExecutor(
      runner_provider=thermos_runner_provider,
      status_providers=status_providers,
      sandbox_provider=UserOverrideDirectorySandboxProvider(options.execute_as_user),
      no_sandbox_create_user=options.no_create_user,
      sandbox_mount_point=options.sandbox_mount_point,
      stop_timeout_in_secs=options.stop_timeout_in_secs
    )
  else:
    thermos_runner_provider = DefaultThermosTaskRunnerProvider(
      dump_runner_pex(),
      checkpoint_root,
      artifact_dir=cwd_path,
      process_logger_destination=options.runner_logger_destination,
      process_logger_mode=options.runner_logger_mode,
      rotate_log_size_mb=options.runner_rotate_log_size_mb,
      rotate_log_backups=options.runner_rotate_log_backups,
      preserve_env=options.preserve_env,
      mesos_containerizer_path=options.mesos_containerizer_path
    )

    thermos_executor = AuroraExecutor(
      runner_provider=thermos_runner_provider,
      status_providers=status_providers,
      no_sandbox_create_user=options.no_create_user,
      sandbox_mount_point=options.sandbox_mount_point,
      stop_timeout_in_secs=options.stop_timeout_in_secs
    )

  return thermos_executor


class ExecutorDriverThread(ExceptionalThread):
  """ Start the executor and wait until it is stopped.

  This is decoupled from the main thread, because only the main thread can receive signals
  and we would miss those when blocking in the driver run method.
  """

  def __init__(self, driver):
    self._driver = driver
    super(ExecutorDriverThread, self).__init__()

  def run(self):
    self._driver.run()


def proxy_main():
  def main(args, options):
    if MesosExecutorDriver is None:
      app.error('Could not load MesosExecutorDriver!')

    thermos_executor = initialize(options)

    # Create driver stub
    driver = MesosExecutorDriver(thermos_executor)

    # This is an ephemeral executor -- shutdown if we receive no tasks within a certain
    # time period
    ExecutorTimeout(thermos_executor.launched, driver).start()

    # Start executor and wait until it is stopped.
    driver_thread = ExecutorDriverThread(driver)
    driver_thread.start()
    try:
      while driver_thread.isAlive():
        driver_thread.join(5)
    except (KeyboardInterrupt, SystemExit):
      driver.stop()
      raise

    log.info('MesosExecutorDriver.run() has finished.')

  app.register_module(ExceptionTerminationHandler())
  app.main()
