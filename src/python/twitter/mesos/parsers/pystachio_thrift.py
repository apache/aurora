import copy
import getpass
import json
from pystachio import Empty, Environment

from gen.twitter.mesos.ttypes import (
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  TwitterTaskInfo,
  UpdateConfig,
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
  ti %= Environment(mesos=context)
  ti %= cluster_context
  return ti.interpolate()


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

  task.numCpus = task_raw.resources().cpu().get()
  task.ramMb = task_raw.resources().ram().get() / MB
  task.diskMb = task_raw.resources().disk().get() / MB
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
    task_copy.thermosConfig = json.dumps(underlying.get())
    tasks.append(task_copy)

  cron_schedule = job.cron_schedule().get() if job.has_cron_schedule() else ''
  cron_policy = CronCollisionPolicy._NAMES_TO_VALUES.get(job.cron_policy().get(), None)
  if cron_policy is None:
    raise ValueError('Invalid cron policy: %s' % job.cron_policy().get())

  update = UpdateConfig()
  update.batchSize = job.update_config().batch_size().get()
  update.restartThreshold = job.update_config().restart_threshold().get()
  update.watchSecs = job.update_config().watch_secs().get()
  update.maxPerShardFailures = job.update_config().max_per_shard_failures().get()
  update.maxTotalFailures = job.update_config().max_total_failures().get()

  if not job.check().ok():
    raise ValueError('Invalid Job Config: %s' % job.check().message())

  return JobConfiguration(
    str(job.name()),
    owner,
    tasks,
    cron_schedule,
    cron_policy,
    update)
