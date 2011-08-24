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

# chroot / task runner                                                                               # workflow=>task
from twitter.thermos.runner import TaskRunner                                                        # Workflow=>Task

class ThermosExecutor(mesos.Executor):
  @staticmethod
  def get_task_from_job(thermos_job, task, replica):                                                 # workflow=>task
    for tsk in thermos_job.tasks:                                                                    # wf=>tsk && workflows=>tasks
      if tsk.name == task and tsk.replica_id == int(replica):                                        # workflow=>task && wf=>tsk && replicaId=>replica_id
        return tsk                                                                                   # wf=>tsk
    print >> sys.stderr, 'unable to find task: %s and replica: %s!\n' % (task, replica)              # workflow=>task
    known_tasks = {}                                                                                 # workflows=>tasks
    for tsk in thermos_job.tasks:                                                                    # wf=>tsk && workflows=>tasks
      if tsk.name not in known_tasks: known_tasks[tsk.name] = []                                     # wf=>tsk && workflows=>tasks
      known_tasks[tsk.name].append(tsk.replica_id)                                                   # wf=>tsk && workflows=>tasks && replicaId=>replica_id
    print >> sys.stderr, 'known task/replicas:'                                                      # workflow=>task
    print >> sys.stderr, pprint.pformat(known_tasks)                                                 # workflows=>tasks

  def __init__(self, options):
    self.sandbox_root = options.sandbox_root
    self.checkpoint_root = options.checkpoint_root
    self.runner = None

  def init(self, driver, args):
    pass

  @staticmethod
  def _boilerplate_lost_process_update(process):                                                     # task=>process
    update = mesos_pb.ProcessStatus()                                                                # Task=>Process
    update.process_id.value = process.process_id.value                                               # task=>process
    update.slave_id.value = process.slave_id.value                                                   # task=>process
    update.state = mesos_pb.TASK_LOST
    return update

  def launchProcess(self, driver, process):                                                          # Task=>Process && task=>process
    print 'Executor[%s]: Got process: %s:%s' % (process.slave_id.value, process.name, process.process_id.value) # task=>process

    if self.runner:
      print 'Error!  Already running a process! %s' % self.runner                                    # task=>process
      driver.sendStatusUpdate(self._boilerplate_lost_process_update(process))                        # task=>process
      return

    process_description = pickle.loads(process.data)                                                 # task=>process
    if 'job' not in process_description or 'task' not in process_description:                        # task=>process && workflow=>task
      driver.sendStatusUpdate(self._boilerplate_lost_process_update(process))                        # task=>process

    self.runner = TaskRunner(                                                                        # Workflow=>Task
      process_description['task'],                                                                   # task=>process && workflow=>task
      self.sandbox_root,
      self.checkpoint_root,
      long(process.process_id.value))                                                                # task=>process
    self.runner.run()
    self.runner = None

    # check status?
    update = mesos_pb.ProcessStatus()                                                                # Task=>Process
    update.process_id.value = process.process_id.value                                               # task=>process
    update.slave_id.value = process.slave_id.value                                                   # task=>process
    update.state = mesos_pb.TASK_FINISHED
    driver.sendStatusUpdate(update)

    # ephemeral executor
    print 'Stopping executor.'
    driver.stop()

  def killProcess(self, driver, processId):                                                          # Task=>Process && task=>process
    print 'Got killProcess %s, ignoring.' % processId                                                # Task=>Process && task=>process

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
                    help = "the path root where we will spawn task sandboxes")                       # workflow=>task
  parser.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
                    default = "/tmp/thermos",
                    help = "the path where we will store task logs and checkpoints")                 # workflow=>task
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
