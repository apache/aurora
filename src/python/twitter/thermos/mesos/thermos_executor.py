import os
import sys
import time
import pprint
import random
import time
from collections import defaultdict
from optparse import OptionParser

# mesos
import mesos
import mesos_pb2 as mesos_pb

# chroot / workflow runner
from twitter.common import app, log
from twitter.thermos.runner import TaskRunner
from gen.twitter.mesos.ttypes import AssignedTask

#thrift
from thrift.TSerialization import deserialize as thrift_deserialize

class ThermosExecutor(mesos.Executor):
  @staticmethod
  def get_task_from_job(thermos_job, task, replica):
    for tsk in thermos_job.tasks:
      if tsk.name == task and tsk.replicaId == int(replica):
        return tsk
    log.error('unable to find task: %s and replica: %s!\n' % (task, replica))
    known_tasks = defaultdict(list)
    for tsk in thermos_job.tasks:
      known_tasks[tsk.name].append(tsk.replicaId)
    log.error('known task/replicas:')
    log.error(pprint.pformat(known_tasks))

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
    log.info('Executor[%s]: Got task: %s:%s' % (task.slave_id.value, task.name, task.task_id.value))

    if self.runner:
      log.error('Error!  Already running a task! %s' % self.runner)
      driver.sendStatusUpdate(self._boilerplate_lost_task_update(task))
      return

    try:
      assigned_task = thrift_deserialize(AssignedTask(), task.data)
      thermos_task = assigned_task.task.thermosConfig
    except Exception as e:
      log.error('Could not deserialize AssignedTask from launchTask!  Stopping driver...')
      driver.stop()
      return

    self.runner = TaskRunner(
      thermos_task,
      self.sandbox_root,
      self.checkpoint_root,
      task.task_id.value)
    self.runner.run()
    self.runner = None

    # check status?
    update = mesos_pb.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = mesos_pb.TASK_FINISHED
    driver.sendStatusUpdate(update)

    # ephemeral executor
    log.info('Stopping executor.')
    driver.stop()
    log.info('After driver.stop()')

  def killTask(self, driver, taskId):
    log.info('Got killTask %s, ignoring.' % taskId)

  def frameworkMessage(self, driver, message):
    log.info('Got frameworkMessage %s, ignoring.' % message)

  def shutdown(self, driver):
    log.info('Got shutdown request, ignoring.')

  def error(self, driver, code, message):
    log.info('Got error, ignoring: %s, %s' % (code, message))


app.add_option("--sandbox_root", dest = "sandbox_root", metavar = "PATH",
               default = "/tmp/thermos/sandbox",
               help = "the path root where we will spawn workflow sandboxes")
app.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
               default = "/tmp/thermos",
               help = "the path where we will store workflow logs and checkpoints")

app.set_option('twitter_common_app_debug', True)
def main(args, options):
  thermos_executor = ThermosExecutor(options)
  drv = mesos.MesosExecutorDriver(thermos_executor)
  drv.run()
  log.info('MesosExecutorDriver.run() has finished.')
  time.sleep(10) # necessary due to ASF MESOS-36

app.main()
