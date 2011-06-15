#!python

import os
import sys
import time
import pprint
import logging

import twitter.common.log
log = twitter.common.log.get()

from twitter.common import options
from twitter.common.recordio import ThriftRecordReader
from twitter.tcl.loader import ThermosJobLoader, MesosJobLoader
from tcl_thrift.ttypes import ThermosJob

from twitter.thermos.runner import WorkflowRunner

def parse_commandline():
  options.add("--thermos", dest = "thermos",
              help = "read thermos job description from .thermos file")
  options.add("--thermos_thrift", dest = "thermos_thrift",
              help = "read thermos job description from stored thrift ThermosJob")
  options.add("--mesos", dest = "mesos",
              help = "translate from mesos job description")
  options.add("--workflow", dest = "workflow", metavar = "WORKFLOW",
              help = "run the workflow by name of WORKFLOW")
  options.add("--replica", dest = "replica_id", metavar = "ID",
              help = "run the replica number ID, from 0 .. number of replicas.")
  options.add("--sandbox_root", dest = "sandbox_root", metavar = "PATH",
              help = "the path root where we will spawn workflow sandboxes")
  options.add("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
              help = "the path where we will store workflow logs and checkpoints")
  options.add("--job_uid", dest = "uid", metavar = "INT64",
              help = "the uid assigned to this task by the scheduler")
  options.add("--action", dest = "action", metavar = "ACTION", default = "run",
              help = "the action for this workflow runner: run, restart, kill")

  (values, args) = options.parse()

  if args:
    log.error("unrecognized arguments: %s\n" % (" ".join(args)))
    options.help()
    sys.exit(1)

  # check invariants
  if values.thermos is None and values.thermos_thrift is None and values.mesos is None:
    log.error("must supply either one of --thermos, --thermos_thrift or --mesos!\n")
    options.print_help(sys.stderr)
    sys.exit(1)

  if not (values.workflow and values.replica_id and values.sandbox_root and values.checkpoint_root and values.uid):
    log.error("ERROR: must supply all of: %s\n" % (
      " ".join(["--workflow", "--replica_id", "--sandbox_root", "--checkpoint_root", "--job_uid"])))
    options.print_help(sys.stderr)
    sys.exit(1)

  return (values, args)

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

def get_workflow_from_job(thermos_job, workflow, replica):
  for wf in thermos_job.workflows:
    if wf.name == workflow and wf.replicaId == int(replica):
      return wf
  log.error('unable to find workflow: %s and replica: %s!\n' % (workflow, replica))
  known_workflows = {}
  for wf in thermos_job.workflows:
    if wf.name not in known_workflows: known_workflows[wf.name] = []
    known_workflows[wf.name].append(wf.replicaId)
  log.info('known workflow/replicas:')
  log.info(pprint.pformat(known_workflows))

def main():
  opts, _ = parse_commandline()

  twitter.common.log.init("thermos_run")

  thermos_replica  = opts.replica_id
  thermos_job      = get_job_from_options(opts)
  thermos_workflow = get_workflow_from_job(thermos_job, opts.workflow, opts.replica_id)

  if thermos_job and thermos_workflow:
    log.info("Woop!  Able to find workflow: %s" % thermos_workflow)
  else:
    log.fatal("Unable to synthesize workflow!")
    sys.exit(1)

  workflow_runner = WorkflowRunner(thermos_workflow,
                                   opts.sandbox_root,
                                   opts.checkpoint_root,
                                   long(opts.uid))

  workflow_runner.run()

if __name__ == '__main__':
  main()
