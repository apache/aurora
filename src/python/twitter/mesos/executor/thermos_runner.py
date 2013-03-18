import functools
import os
import signal
import sys
import traceback

from twitter.common import app, options, log
from twitter.common.log.options import LogOptions

from twitter.thermos.base.options import add_port_to
from twitter.thermos.config.loader import ThermosConfigLoader
from twitter.thermos.runner import TaskRunner


app.add_option("--thermos_json", dest = "thermos_json", default=None,
               help = "read a thermos Task from a serialized json blob")
app.add_option("--sandbox", dest = "sandbox", metavar = "PATH", default=None,
               help = "the sandbox in which this task should run")
app.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH", default=None,
               help = "the path where we will store checkpoints")
app.add_option("--task_id", dest = "task_id", metavar = "STRING", default = None,
               help = "The id to which this task should be bound, created if it does not exist.")
app.add_option("--setuid", dest = "setuid", metavar = "USER", default = None,
               help = "setuid tasks to this user, requires superuser privileges.")
app.add_option("--enable_chroot", dest = "chroot", default = False, action='store_true',
               help = "chroot tasks to the sandbox before executing them.")
app.add_option("--port", type='string', nargs=1, action='callback',
               callback=add_port_to('prebound_ports'), dest='prebound_ports', default={},
               metavar = "NAME:PORT", help = "bind a numbered port PORT to name NAME")

app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_runner')


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


def main(args, opts):
  assert opts.thermos_json and os.path.exists(opts.thermos_json)
  assert opts.sandbox
  assert opts.checkpoint_root

  thermos_task = get_task_from_options(opts)
  prebound_ports = opts.prebound_ports
  missing_ports = set(thermos_task.ports()) - set(prebound_ports)
  if missing_ports:
    app.error('ERROR!  Unbound ports: %s' % ' '.join(port for port in missing_ports))

  task_runner = TaskRunner(thermos_task.task, opts.checkpoint_root, opts.sandbox,
    task_id=opts.task_id, user=opts.setuid, portmap=prebound_ports, chroot=opts.chroot)

  for sig in (signal.SIGUSR1, signal.SIGUSR2):
    signal.signal(sig, functools.partial(runner_teardown, task_runner))

  try:
    task_runner.run()
  except TaskRunner.StateError:
    app.error('Task appears to already be in a terminal state.')
  except TaskRunner.PermissionError:
    app.error('Could not get permission to perform %s!' % opts.action)
  except KeyboardInterrupt:
    runner_teardown(task_runner)


LogOptions.set_simple(True)
app.main()
