#!python
import os
import sys
import threading
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
from twitter.thermos.observer.observer import WorkflowObserver
from twitter.thermos.observer.http import ObserverHttpHandler

class ThermosObservingExecutor(mesos.Executor):
  def __init__(self, options):
    self.checkpoint_root = options.checkpoint_root
    self.port = options.port
    self.observer = None
    self.http = None

  def init(self, driver, args):
    self.hostname = args.hostname

  @staticmethod
  def _boilerplate_lost_task_update(task):
    update = mesos_pb.TaskStatus()
    update.task_id.value = task.task_id.value
    update.slave_id.value = task.slave_id.value
    update.state = mesos_pb.TASK_LOST
    return update

  def launchTask(self, driver, task):
    print 'Got task: %s' % task

    if self.observer:
      print 'Error!  Already running an observer! %s' % self.observer
      driver.sendStatusUpdate(self._boilerplate_lost_task_update(task))
      return

    self.observer = WorkflowObserver(self.checkpoint_root)
    assert self.hostname is not None, "Hostname not set, bailing!"

    def run_http_server():
      self.observer.start()
      self.http = ObserverHttpHandler(self.hostname, self.port, self.observer)
    self.http_thread = threading.Thread(target = run_http_server)
    self.http_thread.start()

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
  parser.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
                    default = "/tmp/thermos",
                    help = "the path where we will store workflow logs and checkpoints")
  parser.add_option("--port", dest = "port", metavar = "PORT",
                    default = 8051,
                    help = "the port on which to register the observer.")
  (options, args) = parser.parse_args()
  return (options, args)


def main():
  options, _ = parse_commandline()
  thermos_executor = ThermosObservingExecutor(options)
  mesos.MesosExecutorDriver(thermos_executor).run()

if __name__ == '__main__':
  main()
