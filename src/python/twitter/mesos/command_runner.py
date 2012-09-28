from multiprocessing.pool import ThreadPool
import subprocess

from twitter.mesos.client_wrapper import MesosClientAPI
from twitter.mesos.config.schema import MesosContext
from twitter.thermos.config.schema import ThermosContext

from pystachio import Ref, Environment, String

from gen.twitter.mesos.constants import LIVE_STATES
from gen.twitter.mesos.ttypes import (
  Identity,
  TaskQuery)


class DistributedCommandRunner(object):
  @staticmethod
  def execute(args):
    hostname, command = args
    ssh_command = ['ssh', '-n', '-q', hostname, command]
    po = subprocess.Popen(ssh_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = po.communicate()
    return '\n'.join('%s:  %s' % (hostname, line) for line in output[0].splitlines())

  @classmethod
  def substitute_thermos(cls, command, task):
    prefix_command = 'cd /var/lib/thermos/{{thermos.task_id}};'
    thermos_namespace = ThermosContext(
        task_id=task.assignedTask.taskId,
        ports=task.assignedTask.assignedPorts,
        user=task.assignedTask.task.owner.role)
    mesos_namespace = MesosContext(
        role=task.assignedTask.task.owner.role,
        instance=task.assignedTask.task.shardId)
    command = String(prefix_command + command) % Environment(
        thermos=thermos_namespace,
        mesos=mesos_namespace)
    return command.get()

  @classmethod
  def substitute_aurora(cls, command, task):
    command = 'cd /var/run/nexus/%task_id%/sandbox;' + command
    command = command.replace('%shard_id%', str(task.assignedTask.task.shardId))
    command = command.replace('%task_id%', task.assignedTask.taskId)
    for name, port in task.assignedTask.assignedPorts.items():
      command = command.replace('%port:' + name + '%', str(port))
    return command

  @classmethod
  def substitute(cls, command, task):
    if task.assignedTask.task.thermosConfig:
      return cls.substitute_thermos(command, task)
    else:
      return cls.substitute_aurora(command, task)

  @classmethod
  def query_from(cls, role, job):
    return TaskQuery(statuses=LIVE_STATES, owner=Identity(role), jobName=job)

  def __init__(self, cluster, role, jobs):
    self._api = MesosClientAPI(cluster=cluster)
    self._role = role
    self._jobs = jobs

  def resolve(self):
    for job in self._jobs:
      resp = self._api.query(self.query_from(self._role, job))
      if not resp.responseCode:
        log.error('Failed to query job: %s' % job)
        continue
      for task in resp.tasks:
        yield task

  def process_arguments(self, command):
    for task in self.resolve():
      host = task.assignedTask.slaveHost
      yield (host, self.substitute(command, task))

  def run(self, command, parallelism=1):
    threadpool = ThreadPool(processes=parallelism)
    for result in threadpool.imap(self.execute, self.process_arguments(command)):
      print result
