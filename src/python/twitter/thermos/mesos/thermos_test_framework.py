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

# loader
from twitter.tcl.loader import ThermosJobLoader

class UidGenerator:
  _singleton = None

  def __init__(self, initial = 1):
    self.uid = initial

  @staticmethod
  def get():
    self = UidGenerator._singleton
    new_uid = self.uid
    self.uid += 1
    return new_uid

UidGenerator._singleton = UidGenerator()

class ThermosFramework(mesos.Scheduler):
  def __init__(self, options):
    self.binary_root = options.binary_root
    self.job = options.job
    self.workflows = list(self.job.workflows) # copy

    # uid => assigned workflow
    self.uid_map = {}

    # hostname => observer id
    self.observers = {}

  def getFrameworkName(self, driver):
    return "Thermos Executor Test Framework"

  def getExecutorInfo(self, driver):
    executor_path = os.path.join(self.binary_root, "thermos_executor.par")
    executor_info = mesos_pb.ExecutorInfo()
    executor_info.executor_id.value = "default"
    executor_info.uri = executor_path
    return executor_info

  def registered(self, driver, fid):
    print "Registered with framework ID %s" % fid.value

  @staticmethod
  def _populate_task_cpu_mem(task, cpu = 1, mem_mb = 1024):
    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb.Resource.SCALAR
    cpus.scalar.value = cpu

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb.Resource.SCALAR
    mem.scalar.value = mem_mb

  def _set_thermos_executor_info(self, task, uid):
    executor_path = os.path.join(self.binary_root, "thermos_executor.par")
    executor_info = mesos_pb.ExecutorInfo()
    executor_info.executor_id.value = "uid:%s" % uid
    executor_info.uri = executor_path
    task.executor.MergeFrom(executor_info)

  def _set_thermos_observer_info(self, task):
    executor_path = os.path.join(self.binary_root, "thermos_observing_executor.par")
    executor_info = mesos_pb.ExecutorInfo()
    executor_info.executor_id.value = "observer"
    executor_info.uri = executor_path
    task.executor.MergeFrom(executor_info)

  def _populate_task(self, task, uid):
    self._populate_task_cpu_mem(task, cpu = 0.3, mem_mb = 512)
    self._set_thermos_executor_info(task, uid)

  def _populate_observer_task(self, task):
    self._populate_task_cpu_mem(task, cpu = 0.1, mem_mb = 64)
    self._set_thermos_observer_info(task)

  def resourceOffer(self, driver, offer_id, offers):
    print "Got resource offer %s" % offer_id.value
    if len(self.workflows) == 0:
      print 'All workflows have been scheduled!'
      return

    tasks = []
    for offer in offers:
      if offer.hostname not in self.observers:
        print 'Detected new executor host %s!  Firing off observer' % offer.hostname
        task = mesos_pb.TaskDescription()
        task.name = 'observer'
        task.task_id.value = str(UidGenerator.get())
        task.slave_id.value = offer.slave_id.value
        self._populate_observer_task(task)
        self.observers[offer.hostname] = offer.slave_id.value
        tasks.append(task)
        continue

      if self.workflows:
        wf = self.workflows[0]

      print "Accepting offer on %s to start task %s:%d" % (
        offer.hostname, wf.name, wf.replicaId)

      task = mesos_pb.TaskDescription()
      task.name = wf.name
      task.task_id.value = str(UidGenerator.get())
      task.slave_id.value = offer.slave_id.value
      self._populate_task(task, task.task_id.value)
      task.data = pickle.dumps( {'job': self.job, 'workflow': wf } )
      self.uid_map[task.task_id.value] = wf
      self.workflows.pop(0)
      tasks.append(task)

    driver.replyToOffer(offer_id, tasks, {})

  def statusUpdate(self, driver, update):
    print 'Task %s is in state %d' % (update.task_id.value, update.state)

def get_job_from_options(options):
  thermos_file = options.thermos
  thermos_job  = ThermosJobLoader(thermos_file).to_thrift()

  if not thermos_job:
    print >> sys.stderr, "Unable to read Thermos job!"
    sys.exit(1)

  return thermos_job

def parse_commandline():
  parser = OptionParser()
  parser.add_option("--binary_root", dest = "binary_root", metavar = "PATH",
                    help = "the path where thermos_scheduler and thermos_executor are located")
  parser.add_option("--thermos", dest = "thermos",
                    help = "read thermos job description from .thermos file")
  (options, args) = parser.parse_args()

  if not options.binary_root:
    print >> sys.stderr, "ERROR: must supply all of: %s\n" % (
      " ".join(["--binary_root"]))
    parser.print_help(sys.stderr)
    sys.exit(1)

  return (options, args)

def main():
  options, args = parse_commandline()
  options.job = get_job_from_options(options)
  thermos_scheduler = ThermosFramework(options)

  mesos.MesosSchedulerDriver(thermos_scheduler, args[0]).run()

if __name__ == '__main__':
  main()
