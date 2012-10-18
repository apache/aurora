from multiprocessing.pool import ThreadPool
import subprocess

from twitter.common import log
from twitter.mesos.client.client_wrapper import MesosClientAPI
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
    hostname, role, command = args
    ssh_command = ['ssh', '-n', '-q', '%s@%s' % (role, hostname), command]
    po = subprocess.Popen(ssh_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = po.communicate()
    return '\n'.join('%s:  %s' % (hostname, line) for line in output[0].splitlines())

  @classmethod
  def thermos_sandbox(cls, executor_sandbox=False):
    if executor_sandbox:
      return '/var/tmp/mesos/slaves/*/frameworks/*/executors/thermos-{{thermos.task_id}}/runs/0'
    else:
      return '/var/lib/thermos/{{thermos.task_id}}'

  @classmethod
  def substitute_thermos(cls, command, task, **kw):
    prefix_command = 'cd %s;' % cls.thermos_sandbox(**kw)
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
  def aurora_sandbox(cls, executor_sandbox=False):
    if executor_sandbox:
      return '/var/tmp/mesos/slaves/*/frameworks/*/executors/twitter/runs/0'
    else:
      return '/var/run/nexus/%task_id%/sandbox'

  @classmethod
  def substitute_aurora(cls, command, task, **kw):
    command = ('cd %s;' % cls.aurora_sandbox(**kw)) + command
    command = command.replace('%shard_id%', str(task.assignedTask.task.shardId))
    command = command.replace('%task_id%', task.assignedTask.taskId)
    for name, port in task.assignedTask.assignedPorts.items():
      command = command.replace('%port:' + name + '%', str(port))
    return command

  @classmethod
  def substitute(cls, command, task, **kw):
    if task.assignedTask.task.thermosConfig:
      return cls.substitute_thermos(command, task, **kw)
    else:
      return cls.substitute_aurora(command, task, **kw)

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

  def process_arguments(self, command, **kw):
    for task in self.resolve():
      host = task.assignedTask.slaveHost
      role = task.assignedTask.task.owner.role
      yield (host, role, self.substitute(command, task, **kw))

  def run(self, command, parallelism=1, **kw):
    threadpool = ThreadPool(processes=parallelism)
    for result in threadpool.imap_unordered(self.execute, self.process_arguments(command, **kw)):
      print result
