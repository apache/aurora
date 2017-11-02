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

import functools
import getpass
import os
import pwd
import signal
import sys
import traceback

from twitter.common import app, log

from apache.thermos.common.excepthook import ExceptionTerminationHandler
from apache.thermos.common.options import add_port_to
from apache.thermos.common.planner import TaskPlanner
from apache.thermos.common.statuses import (
    INTERNAL_ERROR,
    INVALID_TASK,
    TERMINAL_TASK,
    UNKNOWN_ERROR,
    UNKNOWN_USER
)
from apache.thermos.config.loader import ThermosConfigLoader
from apache.thermos.core.process import Process
from apache.thermos.core.runner import TaskRunner

app.add_option(
    "--thermos_json",
    dest="thermos_json",
    default=None,
    help="read a thermos Task from a serialized json blob")


app.add_option(
    "--sandbox",
    dest="sandbox",
    metavar="PATH",
    default=None,
    help="The path on the host filesystem to the sandbox in which this task should run.")


app.add_option(
    '--container_sandbox',
    dest='container_sandbox',
    type=str,
    default=None,
    help='If running in an isolated filesystem, the path within that filesystem where the sandbox '
         'is mounted.')


app.add_option(
     "--checkpoint_root",
     dest="checkpoint_root",
     metavar="PATH",
     default=None,
     help="the path where we will store checkpoints")


app.add_option(
     "--task_id",
     dest="task_id",
     metavar="STRING",
     default=None,
     help="The id to which this task should be bound, created if it does not exist.")


app.add_option(
     "--setuid",
     dest="setuid",
     metavar="USER",
     default=None,
     help="setuid tasks to this user, requires superuser privileges.")


app.add_option(
     "--enable_chroot",
     dest="chroot",
     default=False,
     action='store_true',
     help="chroot tasks to the sandbox before executing them.")


app.add_option(
    "--mesos_containerizer_path",
    dest="mesos_containerizer_path",
    metavar="PATH",
    default=None,
    help="The path to the mesos-containerizer executable that will be used to isolate the task's "
         "filesystem (if using a filesystem image).")


app.add_option(
     "--preserve_env",
     dest="preserve_env",
     default=False,
     action='store_true',
     help="Preserve thermos runners' environment variables for the task being run.")


app.add_option(
     "--port",
     type='string',
     nargs=1,
     action='callback',
     callback=add_port_to('prebound_ports'),
     dest='prebound_ports',
     default={},
     metavar="NAME:PORT",
     help="bind a numbered port PORT to name NAME")


app.add_option(
    "--hostname",
    default=None,
    dest="hostname",
    help="The hostname to advertise in ZooKeeper and the thermos observer instead of "
         "the locally-resolved hostname.")


app.add_option(
    '--process_logger_destination',
    dest='process_logger_destination',
    type=str,
    default=None,
    help='The destination of logger to use for all processes run by thermos.')


app.add_option(
    '--process_logger_mode',
    dest='process_logger_mode',
    type=str,
    default=None,
    help='The logger mode to use for all processes run by thermos.')


app.add_option(
    '--rotate_log_size_mb',
    dest='rotate_log_size_mb',
    type=int,
    default=None,
    help='Maximum size of the rotated stdout/stderr logs emitted by the thermos runner in MiB.')


app.add_option(
    '--rotate_log_backups',
    dest='rotate_log_backups',
    type=int,
    default=None,
    help='Maximum number of rotated stdout/stderr logs emitted by the thermos runner.')


def get_task_from_options(opts):
  tasks = ThermosConfigLoader.load_json(opts.thermos_json)
  if len(tasks.tasks()) == 0:
    app.error("No tasks specified!")
  if len(tasks.tasks()) > 1:
    app.error("Multiple tasks in config but no task name specified!")
  task = tasks.tasks()[0]
  if not task.task.check().ok():
    app.error(task.task.check().message())
  return task


def runner_teardown(runner, sig=signal.SIGUSR1, frame=None):
  """Destroy runner on SIGUSR1 (kill) or SIGUSR2 (lose)"""
  op = 'kill' if sig == signal.SIGUSR1 else 'lose'
  log.info('Thermos runner got signal %s, shutting down.' % sig)
  log.info('Interrupted frame:')
  if frame:
    for line in ''.join(traceback.format_stack(frame)).splitlines():
      log.info(line)
  runner.close_ckpt()
  log.info('Calling runner.%s()' % op)
  getattr(runner, op)()
  sys.exit(0)


class CappedTaskPlanner(TaskPlanner):
  TOTAL_RUN_LIMIT = 100


def proxy_main(args, opts):
  assert opts.thermos_json and os.path.exists(opts.thermos_json)
  assert opts.sandbox
  assert opts.checkpoint_root

  thermos_task = get_task_from_options(opts)
  prebound_ports = opts.prebound_ports
  missing_ports = set(thermos_task.ports()) - set(prebound_ports)

  if missing_ports:
    log.error('ERROR!  Unbound ports: %s' % ' '.join(port for port in missing_ports))
    sys.exit(INTERNAL_ERROR)

  if opts.setuid:
    user = opts.setuid
  else:
    user = getpass.getuser()

  # if we cannot get the uid, this is an unknown user and we should fail
  try:
    pwd.getpwnam(user).pw_uid
  except KeyError:
    log.error('Unknown user: %s' % user)
    sys.exit(UNKNOWN_USER)

  task_runner = TaskRunner(
      thermos_task.task,
      opts.checkpoint_root,
      opts.sandbox,
      task_id=opts.task_id,
      user=opts.setuid,
      portmap=prebound_ports,
      chroot=opts.chroot,
      planner_class=CappedTaskPlanner,
      hostname=opts.hostname,
      process_logger_destination=opts.process_logger_destination,
      process_logger_mode=opts.process_logger_mode,
      rotate_log_size_mb=opts.rotate_log_size_mb,
      rotate_log_backups=opts.rotate_log_backups,
      preserve_env=opts.preserve_env,
      mesos_containerizer_path=opts.mesos_containerizer_path,
      container_sandbox=opts.container_sandbox)

  for sig in (signal.SIGUSR1, signal.SIGUSR2):
    signal.signal(sig, functools.partial(runner_teardown, task_runner))

  try:
    task_runner.run()
  except TaskRunner.InternalError as err:
    log.error('Internal error: %s' % err)
    sys.exit(INTERNAL_ERROR)
  except TaskRunner.InvalidTask as err:
    log.error('Invalid task: %s' % err)
    sys.exit(INVALID_TASK)
  except TaskRunner.StateError as err:
    log.error('Checkpoint error: %s' % err)
    sys.exit(TERMINAL_TASK)
  except Process.UnknownUserError as err:
    log.error('User ceased to exist: %s' % err)
    sys.exit(UNKNOWN_USER)
  except KeyboardInterrupt:
    log.info('Caught ^C, tearing down runner.')
    runner_teardown(task_runner)
  except Exception as e:
    log.error('Unknown exception: %s' % e)
    for line in traceback.format_exc().splitlines():
      log.error(line)
    sys.exit(UNKNOWN_ERROR)


def main(args, opts):
  return proxy_main(args, opts)


app.register_module(ExceptionTerminationHandler())
app.main()
