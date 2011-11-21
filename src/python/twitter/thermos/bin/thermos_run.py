import os
import sys
import time
import pprint

from twitter.common import app, log, options
from twitter.common.recordio import ThriftRecordReader
from twitter.tcl.loader import ThermosJobLoader, MesosJobLoader
from gen.twitter.tcl.ttypes import ThermosTask

from twitter.thermos.runner import TaskRunner

app.add_option("--thermos", dest = "thermos",
               help = "read thermos job description from .thermos file")
app.add_option("--thermos_thrift", dest = "thermos_thrift",
               help = "read ThermosTask from a serialized thrift blob")
app.add_option("--mesos", dest = "mesos",
               help = "translate from mesos job description")
app.add_option("--task", dest = "task", metavar = "TASK",
               help = "run the task by name of TASK")
app.add_option("--replica", dest = "replica_id", metavar = "ID",
               help = "run the replica number ID, from 0 .. number of replicas.")
app.add_option("--sandbox", dest = "sandbox", metavar = "PATH",
               help = "the sandbox in which this task should run")
app.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
               help = "the path where we will store task logs and checkpoints")
app.add_option("--task_id", dest = "uid", metavar = "STRING",
               help = "the uid assigned to this task by the scheduler")
app.add_option("--action", dest = "action", metavar = "ACTION", default = "run",
               help = "the action for this task runner: run, kill")
app.add_option("--setuid", dest = "setuid", metavar = "USER", default = None,
               help = "setuid tasks to this user, requires superuser privileges.")
app.add_option("--enable_chroot", dest = "chroot", default = False, action='store_true',
               help = "chroot tasks to the sandbox before executing them.")

def check_invariants(args, values):
  if args:
    app.error("unrecognized arguments: %s\n" % (" ".join(args)))

  # check invariants
  if values.thermos is None and values.thermos_thrift is None and values.mesos is None:
    app.error("must supply either one of --thermos, --thermos_thrift or --mesos!\n")

  if (values.mesos is not None or values.thermos is not None) and (
      values.task is None or values.replica_id is None):
    app.error('If specifying a Mesos or Thermos job, must also specify task and replica.')

  if not (values.sandbox and values.checkpoint_root and values.uid):
    app.error("ERROR: must supply sandbox, checkpoint_root and task_id")

def get_task_from_job(thermos_job, task, replica):
  for tsk in thermos_job.tasks:
    if tsk.name == task and tsk.replica_id == int(replica):
      return tsk
  log.error('unable to find task: %s and replica: %s!\n' % (task, replica))
  known_tasks = {}
  for tsk in thermos_job.tasks:
    if tsk.name not in known_tasks: known_tasks[tsk.name] = []
    known_tasks[tsk.name].append(tsk.replica_id)
  log.info('known task/replicas:')
  log.info(pprint.pformat(known_tasks))

def get_task_from_options(opts):
  thermos_job = None

  if opts.thermos_thrift:
    with open(opts.thermos_thrift) as thermos_fd:
      rr = ThriftRecordReader(thermos_fd, ThermosTask)
      thermos_task = rr.read()
    if thermos_task is None:
      log.fatal("Unable to read Thermos task from thrift blob!")
      sys.exit(1)
    return thermos_task

  if opts.thermos:
    thermos_job = ThermosJobLoader(opts.thermos).to_thrift()
  elif opts.mesos:
    thermos_job = MesosJobLoader(opts.mesos).to_thrift()

  if thermos_job is None:
    log.fatal("Unable to read Thermos job!")
    sys.exit(1)

  thermos_task = get_task_from_job(thermos_job, opts.task, opts.replica_id)
  if thermos_task is None:
    log.fatal("Unable to get Thermos task from job!")
    sys.exit(1)

  return thermos_task

def main(args, opts):
  check_invariants(args, opts)

  thermos_task = get_task_from_options(opts)

  # TODO(wickman):  Need a general sanitizing suite for uids, job names, etc.
  task_runner = TaskRunner(thermos_task, opts.sandbox, opts.checkpoint_root, opts.uid,
    opts.setuid, opts.chroot)
  if opts.action == 'run':
    task_runner.run()
  elif opts.action == 'kill':
    task_runner.kill()

app.main()
