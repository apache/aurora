import os
import sys
import time
import pprint

from twitter.common import app, log, options
from twitter.thermos.config.loader import ThermosConfigLoader
from twitter.thermos.runner import TaskRunner

app.add_option("--thermos", dest = "thermos",
               help = "read thermos job description from .thermos file")
app.add_option("--thermos_json", dest = "thermos_json",
               help = "read a thermos Task from a serialized json blob")

app.add_option("--task", dest = "task", metavar = "NAME", default=None,
               help = "run the task by the name of NAME")

app.add_option("--sandbox", dest = "sandbox", metavar = "PATH",
               help = "the sandbox in which this task should run")
app.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
               help = "the path where we will store task logs and checkpoints")

app.add_option("--task_id", dest = "task_id", metavar = "STRING", default = None,
               help = "The id to which this task should be bound, created if it does not exist.")

app.add_option("--action", dest = "action", metavar = "ACTION", default = "run",
               help = "the action for this task runner: run, kill")

app.add_option("--setuid", dest = "setuid", metavar = "USER", default = None,
               help = "setuid tasks to this user, requires superuser privileges.")
app.add_option("--enable_chroot", dest = "chroot", default = False, action='store_true',
               help = "chroot tasks to the sandbox before executing them.")

app.add_option("--force", dest = "force", default = False, action='store_true',
               help = "If another runner owns the checkpoint, force kill it in order to take over "
                      "runner duties.")


def add_port_callback(option, opt, value, parser):
  if not hasattr(parser.values, 'prebound_ports'):
    parser.values.prebound_ports = []
  parser.values.prebound_ports.append(value)
app.add_option("--port", type='string', nargs=1,
               action='callback', callback=add_port_callback,
               default=[], metavar = "NAME:PORT",
               help = "an indication to bind a numbered port PORT to name NAME")

def check_invariants(args, values):
  if args:
    app.error("unrecognized arguments: %s\n" % (" ".join(args)))

  # check invariants
  if values.thermos is None and values.thermos_json is None:
    app.error("must supply either one of --thermos, --thermos_json!\n")

  if not (values.sandbox and values.checkpoint_root):
    app.error("ERROR: must supply sandbox, checkpoint_root and task_id")


def get_task_from_options(opts):
  if opts.thermos_json:
    tasks = ThermosConfigLoader.load_json(opts.thermos_json)
  else:
    tasks = ThermosConfigLoader.load(opts.thermos)

  if len(tasks.tasks()) == 0:
    app.error("No tasks specified!")

  if opts.task is None and len(tasks.tasks()) > 1:
    app.error("Multiple tasks in config but no task name specified!")

  task = None
  if opts.task is not None:
    for t in tasks.tasks():
      if t.task().name().get() == opts.task:
        task = t
        break
    if task is None:
      app.error("Could not find task %s!" % opts.task)
  else:
    task = tasks.tasks()[0]

  if not task.task.check().ok():
    app.error(task.task.check().message())

  return task


def get_prebound_ports(opts):
  ports = {}
  if hasattr(opts, 'prebound_ports'):
    for value in opts.prebound_ports:
      try:
        name, port = value.split(':')
      except (ValueError, TypeError):
        app.error('Invalid value for --port: %s, should be of form NAME:PORT' % value)
      try:
        port = int(port)
      except ValueError:
        app.error('Invalid value for --port: %s, could not coerce port number to integer.' % value)
      ports[name] = port
  return ports


def main(args, opts):
  check_invariants(args, opts)

  thermos_task = get_task_from_options(opts)
  prebound_ports = get_prebound_ports(opts)
  missing_ports = set(thermos_task.ports()) - set(prebound_ports.keys())
  if missing_ports:
    app.error('ERROR!  Unbound ports: %s' % ' '.join(port for port in missing_ports))

  task_runner = TaskRunner(thermos_task.task, opts.checkpoint_root, opts.sandbox,
    task_id=opts.task_id, user=opts.setuid, portmap=prebound_ports, chroot=opts.chroot)

  try:
    if opts.action == 'run':
      task_runner.run(opts.force)
    elif opts.action == 'kill':
      task_runner.kill(opts.force)
  except TaskRunner.StateError:
    app.error('Task appears to already be in a terminal state.')

app.main()
