import os
import sys
import time
import pprint

from twitter.common import app, log, options
from twitter.common.recordio import ThriftRecordReader
from twitter.tcl.loader import ThermosJobLoader, MesosJobLoader
from gen.twitter.tcl.ttypes import ThermosJob

from twitter.thermos.runner import TaskRunner

app.add_option("--thermos", dest = "thermos",
               help = "read thermos job description from .thermos file")
app.add_option("--thermos_thrift", dest = "thermos_thrift",
               help = "read thermos job description from stored thrift ThermosJob")
app.add_option("--mesos", dest = "mesos",
               help = "translate from mesos job description")
app.add_option("--task", dest = "task", metavar = "TASK",
               help = "run the task by name of TASK")
app.add_option("--replica", dest = "replica_id", metavar = "ID",
               help = "run the replica number ID, from 0 .. number of replicas.")
app.add_option("--sandbox_root", dest = "sandbox_root", metavar = "PATH",
               help = "the path root where we will spawn task sandboxes")
app.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
               help = "the path where we will store task logs and checkpoints")
app.add_option("--task_id", dest = "uid", metavar = "STRING",
               help = "the uid assigned to this task by the scheduler")
app.add_option("--action", dest = "action", metavar = "ACTION", default = "run",
               help = "the action for this task runner: run, kill")


def check_invariants(args, values):
  if args:
    log.error("unrecognized arguments: %s\n" % (" ".join(args)))
    app.help()
    sys.exit(1)

  # check invariants
  if values.thermos is None and values.thermos_thrift is None and values.mesos is None:
    log.error("must supply either one of --thermos, --thermos_thrift or --mesos!\n")
    app.help()
    sys.exit(1)

  if not (values.task and values.replica_id and values.sandbox_root and (
      values.checkpoint_root and values.uid)):
    log.error("ERROR: must supply all of: %s\n" % (
      " ".join(["--task", "--replica_id", "--sandbox_root", "--checkpoint_root", "--task_id"])))
    app.help()
    sys.exit(1)

def get_job_from_options(opts):
  thermos_job = None

  if opts.thermos_thrift:
    thermos_file    = opts.thermos_thrift
    thermos_file_fd = file(thermos_file, "r")
    rr              = ThriftRecordReader(thermos_file_fd, ThermosJob)
    thermos_job     = rr.read()

  elif opts.thermos:
    thermos_file   = opts.thermos
    thermos_job    = ThermosJobLoader(thermos_file).to_thrift()

  elif opts.mesos:
    mesos_file     = opts.mesos
    thermos_job    = MesosJobLoader(mesos_file).to_thrift()

  if not thermos_job:
    log.fatal("Unable to read Thermos job!")
    sys.exit(1)

  return thermos_job

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

def main(args, opts):
  check_invariants(args, opts)

  thermos_replica = opts.replica_id
  thermos_job = get_job_from_options(opts)
  thermos_task = get_task_from_job(thermos_job, opts.task, opts.replica_id)

  if not thermos_job or not thermos_task:
    log.fatal("Unable to synthesize task!")
    sys.exit(1)

  # TODO(wickman):  Need a general sanitizing suite for uids, job names, etc.
  task_runner = TaskRunner(thermos_task, opts.sandbox_root, opts.checkpoint_root, opts.uid)
  if opts.action == 'run':
    task_runner.run()
  elif opts.action == 'kill':
    task_runner.kill()

app.main()
