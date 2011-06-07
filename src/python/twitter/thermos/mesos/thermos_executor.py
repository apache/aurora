#!python

import os
import sys
import time
import pprint
import pickle
from optparse import OptionParser

# mesos
try:
  sys.path = ['/opt/local/mesos/lib/python',
              '/usr/local/mesos/lib/python'] + sys.path
  import mesos
  import mesos_pb2 as mesos_pb
except ImportError:
  print 'Could not import mesos framework!'
  sys.exit(1)

# chroot / workflow runner
from twitter.thermos.runner import WorkflowRunner

class ThermosExecutor(mesos.Executor):
  @staticmethod
  def get_workflow_from_job(thermos_job, workflow, replica):
    for wf in thermos_job.workflows:
      if wf.name == workflow and wf.replicaId == int(replica):
        return wf
    print >> sys.stderr, 'unable to find workflow: %s and replica: %s!\n' % (workflow, replica)
    known_workflows = {}
    for wf in thermos_job.workflows:
      if wf.name not in known_workflows: known_workflows[wf.name] = []
      known_workflows[wf.name].append(wf.replicaId)
    print >> sys.stderr, 'known workflow/replicas:'
    print >> sys.stderr, pprint.pformat(known_workflows)

  def __init__(self, options):
    self.sandbox_root = options.sandbox_root
    self.checkpoint_root = options.checkpoint_root
    self.runner = None

  def init(self, driver, args):
    pass

  @staticmethod
  def _boilerplate_lost_task_update(task):
    update = mesos_pb.TaskStatus()
    update.task_id.value = task.task_id.value
    update.slave_id.value = task.slave_id.value
    update.state = mesos_pb.TASK_LOST
    return update

  def launchTask(self, driver, task):
    print 'Executor[%s]: Got task: %s:%s' % (task.slave_id.value, task.name, task.task_id.value)

    if self.runner:
      print 'Error!  Already running a task! %s' % self.runner
      driver.sendStatusUpdate(self._boilerplate_lost_task_update(task))
      return

    task_description = pickle.loads(task.data)
    if 'job' not in task_description or 'workflow' not in task_description:
      driver.sendStatusUpdate(self._boilerplate_lost_task_update(task))

    self.runner = WorkflowRunner(
      task_description['workflow'],
      self.sandbox_root,
      self.checkpoint_root,
      long(task.task_id.value))
    self.runner.run()
    self.runner = None

    # check status?
    update = mesos_pb.TaskStatus()
    update.task_id.value = task.task_id.value
    update.slave_id.value = task.slave_id.value
    update.state = mesos_pb.TASK_FINISHED
    driver.sendStatusUpdate(update)

    # ephemeral executor
    print 'Stopping executor.'
    driver.stop()

  def killTask(self, driver, taskId):
    print 'Got killTask %s, ignoring.' % taskId

  def frameworkMessage(self, driver, message):
    print 'Got frameworkMessage %s, ignoring.' % message

  def shutdown(self, driver):
    print 'Got shutdown request, ignoring.'

  def error(self, driver, code, message):
    print 'Got error, ignoring: %s, %s' % (code, message)

def parse_commandline():
  parser = OptionParser()
  parser.add_option("--sandbox_root", dest = "sandbox_root", metavar = "PATH",
                    default = "/tmp/thermos/sandbox",
                    help = "the path root where we will spawn workflow sandboxes")
  parser.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
                    default = "/tmp/thermos",
                    help = "the path where we will store workflow logs and checkpoints")
  (options, args) = parser.parse_args()

  if args:
    print >> sys.stderr, "ERROR: unrecognized arguments: %s\n" % (" ".join(args))
    parser.print_help(sys.stderr)
    sys.exit(1)

  if not (options.sandbox_root and options.checkpoint_root):
    print >> sys.stderr, "ERROR: must supply all of: %s\n" % (
      " ".join(["--sandbox_root", "--checkpoint_root"]))
    parser.print_help(sys.stderr)
    sys.exit(1)

  return (options, args)


def main():
  options, _ = parse_commandline()
  thermos_executor = ThermosExecutor(options)
  mesos.MesosExecutorDriver(thermos_executor).run()

if __name__ == '__main__':
  main()
