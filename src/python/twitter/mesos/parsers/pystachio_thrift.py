import copy
import getpass
import json
from pystachio import Empty, Environment

from gen.twitter.mesos.ttypes import (
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  TwitterTaskInfo,
)

from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import (
  MesosContext,
  MesosTaskInstance
)
from twitter.thermos.config.loader import ThermosTaskWrapper

from .base import ThriftCodec

__all__ = (
  'InvalidConfig',
  'convert'
)


def task_instance_from_job(job, instance):
  context = MesosContext(role=job.role(), instance=instance, cluster=job.cluster())
  cluster_context = Cluster.get(job.cluster().get()).context()
  ti = MesosTaskInstance(task=job.task(),
                         layout=job.layout(),
                         role=job.role(),
                         health_check_interval_secs=job.health_check_interval_secs(),
                         instance=instance)
  return ti.bind(Environment(mesos=context), cluster_context).interpolate()


def convert(job):
  uninterpolated_vars = set()

  if not job.role().check().ok():
    raise ValueError(job.role().check().message())

  owner = Identity(role=str(job.role()), user=getpass.getuser())

  task_raw = job.task()

  MB = 1024 * 1024
  task = TwitterTaskInfo()

  # job components
  task.jobName = job.name().get()
  task.production = bool(job.production().get())
  task.isDaemon = bool(job.daemon().get())
  task.maxTaskFailures = job.max_task_failures().get()
  task.priority = job.priority().get()
  if job.has_health_check_interval_secs():
    task.healthCheckIntervalSecs = job.health_check_interval_secs().get()

  # task components
  if not task_raw.has_resources():
    raise ValueError('Task must specify resources!')

  if task_raw.resources().ram().get() == 0 or task_raw.resources().disk().get() == 0:
    raise ValueError('Must specify ram and disk resources, got ram:%r disk:%r' % (
      task_raw.resources().ram().get(), task_raw.resources().disk().get()))

  task.numCpus = task_raw.resources().cpu().get()
  task.ramMb = task_raw.resources().ram().get() / MB
  task.diskMb = task_raw.resources().disk().get() / MB
  if task.numCpus <= 0 or task.ramMb <= 0 or task.diskMb <= 0:
    raise ValueError('Task has invalid resources.  cpu/ramMb/diskMb must all be positive: '
        'cpu:%r ramMb:%r diskMb:%r' % (task.numCpus, task.ramMb, task.diskMb))

  task.owner = owner
  task.requestedPorts = ThermosTaskWrapper(task_raw, strict=False).ports()
  task.constraints = ThriftCodec.constraints_to_thrift(
    job.constraints().get() if job.constraints() is not Empty else {})

  # Replicate task objects to reflect number of instances.
  tasks = []
  for k in range(job.instances().get()):
    task_copy = copy.deepcopy(task)
    task_copy.shardId = k
    underlying, _ = task_instance_from_job(job, k)
    if not underlying.check().ok():
      raise ValueError('Task not fully specified: %s' % underlying.check().message())
    task_copy.thermosConfig = json.dumps(underlying.get())
    tasks.append(task_copy)

  cron_schedule = job.cron_schedule().get() if job.has_cron_schedule() else ''
  cron_policy = CronCollisionPolicy._NAMES_TO_VALUES.get(job.cron_policy().get())
  if cron_policy is None:
    raise ValueError('Invalid cron policy: %s' % job.cron_policy().get())

  return JobConfiguration(
    str(job.name()),
    owner,
    tasks,
    cron_schedule,
    cron_policy)
