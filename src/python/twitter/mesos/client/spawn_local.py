import os
import socket
import subprocess
import tempfile
import threading
import time
import urllib2
import webbrowser

from twitter.common import log
from twitter.common.contextutil import temporary_dir
from twitter.common.http import HttpServer
from twitter.common.net.tunnel import TunnelHelper
from twitter.mesos.client.client_util import die, get_config
from twitter.mesos.executor.sandbox_manager import AppAppSandbox, DirectorySandbox
from twitter.mesos.executor.task_runner_wrapper import TaskRunnerWrapper
from twitter.mesos.executor.thermos_executor import ThermosExecutor
from twitter.mesos.parsers.pystachio_config import PystachioConfig
from twitter.thermos.observer.observer import TaskObserver
from twitter.thermos.observer.http import BottleObserver

from mesos import ExecutorDriver
import mesos_pb2 as mesos_pb


def spawn_observer(checkpoint_root):
  server = HttpServer()
  observer = TaskObserver(checkpoint_root)
  observer.start()
  server.mount_routes(BottleObserver(observer))
  random_port = TunnelHelper.get_random_port()
  class ServerThread(threading.Thread):
    def run(self):
      server.run('0.0.0.0', random_port, 'cherrypy')
  server_thread = ServerThread()
  server_thread.daemon = True
  server_thread.start()
  return server_thread, random_port


def wait_and_spawn_browser(url):
  # wait for the task to be registered
  end = time.time() + 5.0
  while time.time() < end:
    try:
      urllib2.urlopen(url)
    except (urllib2.HTTPError, urllib2.URLError) as e:
      time.sleep(0.5)
    else:
      webbrowser.open_new_tab(url)
      break
  else:
    log.error("Timed out waiting for task to be registered to observer")


def make_local_runner_wrapper(runner_pex, sandbox_root, checkpoint_root):
  class LocalTaskRunner(TaskRunnerWrapper):
    def __init__(self, task_id, mesos_task, role, mesos_ports, **kw):
      if mesos_task.has_layout():
        if os.uname()[0] != 'Linux':
          raise self.TaskError("app-app sandboxes are only available on Linux")
        sandbox = AppAppSandbox(task_id)
        enable_chroot = True
      else:
        sandbox = DirectorySandbox(os.path.join(sandbox_root, task_id, 'sandbox'))
        enable_chroot = False
      super(LocalTaskRunner, self).__init__(
        task_id, mesos_task, role, mesos_ports, runner_pex, sandbox,
        artifact_dir=sandbox_root,
        checkpoint_root=checkpoint_root,
        **kw)
      self._enable_chroot = enable_chroot
  return LocalTaskRunner


def build_local_runner():
  expected_pex = os.path.join('dist', 'thermos_runner.pex')
  rc = subprocess.call(['./pants', 'src/python/twitter/mesos/executor:thermos_runner'])
  if rc == 0 and os.path.exists(expected_pex):
    return expected_pex
  return None


def create_executor(runner_pex, sandbox_root, checkpoint_root):
  return ThermosExecutor(runner_class=make_local_runner_wrapper(
      runner_pex, sandbox_root, checkpoint_root))


def pick_task(job_configuration, shard_id=0):
  for tti in job_configuration.taskConfigs:
    if tti.shardId == shard_id:
      return tti
  raise ValueError('shard_id %s not part of job!' % shard_id)


def create_assigned_ports(names):
  return dict((name, TunnelHelper.get_random_port()) for name in names)


class LocalDriver(ExecutorDriver):
  def __init__(self, *args, **kw):
    self.stopped = threading.Event()
    # TODO(wickman) Add __init__ to ExecutorDriver in mesos core.
    # ExecutorDriver.__init__(self, *args, **kw)
    self.started = threading.Event()

  def stop(self):
    log.info('LocalDriver.stop called.')
    self.stopped.set()

  def sendStatusUpdate(self, status):
    log.info('LocalDriver.sendStatusUpdate(%s)' % status)
    if status.state == mesos_pb.TASK_RUNNING:
      self.started.set()

  def sendFrameworkMessage(self, data):
    log.info('LocalDriver.sendFrameworkMessage(%s)' % data)


def create_taskinfo(proxy_config, shard_id=0):
  import socket
  from mesos_pb2 import (
      TaskInfo,
      TaskID,
      SlaveID)
  from gen.twitter.mesos.ttypes import AssignedTask
  from thrift.TSerialization import serialize as thrift_serialize

  job_configuration = proxy_config.job()
  tti = pick_task(job_configuration, shard_id)
  local_time = time.strftime('%Y%m%d-%H%M%S')
  task_id = slave_id = job_configuration.name + local_time

  assigned_task = AssignedTask(
    taskId=task_id,
    slaveId=slave_id,
    slaveHost=socket.gethostname(),
    task=tti,
    assignedPorts=create_assigned_ports(proxy_config.ports()))

  task_info = TaskInfo(
    name=job_configuration.name,
    task_id=TaskID(value=job_configuration.name + local_time),
    slave_id=SlaveID(value=job_configuration.name + local_time),
    data=thrift_serialize(assigned_task))

  return task_info


def spawn_local(runner, jobname, config_file, json=False, open_browser=False,
                shard=0, bindings=()):
  """
    Spawn a local run of a task.
  """
  try:
    config = get_config(jobname, config_file, json, force_local=True,
        bindings=bindings, translate=True)
  except PystachioConfig.InvalidConfig as err:
    die("Invalid configuration: %s" % err)

  checkpoint_root = os.path.expanduser(os.path.join('~', '.thermos'))
  _, port = spawn_observer(checkpoint_root)
  task_info = create_taskinfo(config, shard)

  with temporary_dir() as sandbox:
    runner_pex = runner if runner != 'build' else build_local_runner()
    if runner_pex is None:
      print('failed to build thermos runner!')
      return 1

    executor = create_executor(runner_pex, sandbox, checkpoint_root)
    driver = LocalDriver()
    executor.launchTask(driver, task_info)

    # wait for the driver to start
    driver.started.wait(timeout=5.0)

    if open_browser:
      wait_and_spawn_browser(
        'http://%s:%d/task/%s' % (socket.gethostname(), port, task_info.task_id.value))

    try:
      driver.stopped.wait()
    except KeyboardInterrupt:
      print('Got interrupt, killing task.')

    executor.shutdown(driver)

  print('Local spawn completed.')
  return 0
