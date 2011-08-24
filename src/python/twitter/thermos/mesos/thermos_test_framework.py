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

class UidGenerator(object):
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
    self.tasks = list(self.job.tasks) # copy                                                         # workflows=>tasks

    # uid => assigned task                                                                           # workflow=>task
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
  def _populate_process_cpu_mem(process, cpu = 1, mem_mb = 1024):                                    # task=>process
    cpus = process.resources.add()                                                                   # task=>process
    cpus.name = "cpus"
    cpus.type = mesos_pb.Resource.SCALAR
    cpus.scalar.value = cpu

    mem = process.resources.add()                                                                    # task=>process
    mem.name = "mem"
    mem.type = mesos_pb.Resource.SCALAR
    mem.scalar.value = mem_mb

  def _set_thermos_executor_info(self, process, uid):                                                # task=>process
    executor_path = os.path.join(self.binary_root, "thermos_executor.par")
    executor_info = mesos_pb.ExecutorInfo()
    executor_info.executor_id.value = "uid:%s" % uid
    executor_info.uri = executor_path
    process.executor.MergeFrom(executor_info)                                                        # task=>process

  def _set_thermos_observer_info(self, process):                                                     # task=>process
    executor_path = os.path.join(self.binary_root, "thermos_observing_executor.par")
    executor_info = mesos_pb.ExecutorInfo()
    executor_info.executor_id.value = "observer"
    executor_info.uri = executor_path
    process.executor.MergeFrom(executor_info)                                                        # task=>process

  def _populate_process(self, process, uid):                                                         # task=>process
    self._populate_process_cpu_mem(process, cpu = 0.3, mem_mb = 512)                                 # task=>process
    self._set_thermos_executor_info(process, uid)                                                    # task=>process

  def _populate_observer_process(self, process):                                                     # task=>process
    self._populate_process_cpu_mem(process, cpu = 0.1, mem_mb = 64)                                  # task=>process
    self._set_thermos_observer_info(process)                                                         # task=>process

  def resourceOffer(self, driver, offer_id, offers):
    print "Got resource offer %s" % offer_id.value
    if len(self.tasks) == 0:                                                                         # workflows=>tasks
      print 'All tasks have been scheduled!'                                                         # workflows=>tasks
      return

    processes = []                                                                                   # tasks=>processes
    for offer in offers:
      if offer.hostname not in self.observers:
        print 'Detected new executor host %s!  Firing off observer' % offer.hostname
        process = mesos_pb.ProcessDescription()                                                      # Task=>Process && task=>process
        process.name = 'observer'                                                                    # task=>process
        process.process_id.value = str(UidGenerator.get())                                           # task=>process
        process.slave_id.value = offer.slave_id.value                                                # task=>process
        self._populate_observer_process(process)                                                     # task=>process
        self.observers[offer.hostname] = offer.slave_id.value
        processes.append(process)                                                                    # task=>process && tasks=>processes
        continue

      if self.tasks:                                                                                 # workflows=>tasks
        tsk = self.tasks[0]                                                                          # wf=>tsk && workflows=>tasks

      print "Accepting offer on %s to start process %s:%d" % (                                       # task=>process
        offer.hostname, tsk.name, tsk.replica_id)                                                    # wf=>tsk && replicaId=>replica_id

      process = mesos_pb.ProcessDescription()                                                        # Task=>Process && task=>process
      process.name = tsk.name                                                                        # task=>process && wf=>tsk
      process.process_id.value = str(UidGenerator.get())                                             # task=>process
      process.slave_id.value = offer.slave_id.value                                                  # task=>process
      self._populate_process(process, process.process_id.value)                                      # task=>process
      process.data = pickle.dumps( {'job': self.job, 'task': tsk } )                                 # task=>process && workflow=>task && wf=>tsk
      self.uid_map[process.process_id.value] = tsk                                                   # task=>process && wf=>tsk
      self.tasks.pop(0)                                                                              # workflows=>tasks
      processes.append(process)                                                                      # task=>process && tasks=>processes

    driver.replyToOffer(offer_id, processes, {})                                                     # tasks=>processes

  def statusUpdate(self, driver, update):
    print 'Process %s is in state %d' % (update.process_id.value, update.state)                      # Task=>Process && task=>process

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
