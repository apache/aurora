import copy
import getpass
import json
import sys

from gen.twitter.mesos.ttypes import (
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  TwitterTaskInfo,
)

from twitter.mesos.config.schema import (
  MesosContext,
  MesosTaskInstance
)

from twitter.thermos.config.loader import (
  ThermosTaskValidator,
  ThermosTaskWrapper)

from .base import ThriftCodec

from pystachio import Empty, Environment

__all__ = (
  'InvalidConfig',
  'convert'
)


def task_instance_from_job(job, instance):
  instance_context = MesosContext(instance=instance)
  ti = MesosTaskInstance(task=job.task(),
                         layout=job.layout(),
                         role=job.role(),
                         health_check_interval_secs=job.health_check_interval_secs(),
                         instance=instance)
  if job.has_announce():
    ti = ti(announce=job.announce())
  return ti.bind(mesos = instance_context).interpolate()


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

  def not_empty_or(item, default):
    return default if item is Empty else item.get()

  task.taskLinks = not_empty_or(job.task_links(), {})
  task.constraints = ThriftCodec.constraints_to_thrift(not_empty_or(job.constraints(), {}))

  # Replicate task objects to reflect number of instances.
  tasks = []
  for k in range(job.instances().get()):
    task_copy = copy.deepcopy(task)
    task_copy.shardId = k
    underlying, refs = task_instance_from_job(job, k)
    try:
      ThermosTaskValidator.assert_valid_task(underlying.task())
    except ThermosTaskValidator.InvalidTaskError as e:
      raise ValueError('Task is invalid: %s' % e)
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
