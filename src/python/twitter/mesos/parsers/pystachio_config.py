import copy
import getpass
import json
import sys

from pystachio import Empty, Environment, Ref
from twitter.common.dirutil import safe_open
from twitter.common.lang import Compatibility
from twitter.mesos.config.schema import (
  MesosContext,
  MesosJob,
  MesosTaskInstance
)

from twitter.thermos.config.loader import ThermosTaskWrapper
from gen.twitter.mesos.ttypes import (
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  TwitterTaskInfo,
  UpdateConfig,
)

from .base import ThriftCodec
from .proxy_config import ProxyConfig


SCHEMA_PREAMBLE = """
from pystachio import *
from twitter.mesos.config.schema import *
from twitter.thermos.config.dsl import *
"""

def deposit_schema(environment):
  Compatibility.exec_function(
    compile(SCHEMA_PREAMBLE, "<exec_function>", "exec"), environment)

class MesosConfigLoader(object):
  SCHEMA = {}
  deposit_schema(SCHEMA)

  class BadConfig(Exception): pass

  @staticmethod
  def pick(job_list, name, bindings):
    if not job_list:
      raise ValueError('No jobs specified!')
    if name is None:
      if len(job_list) > 1:
        raise ValueError('Configuration has multiple jobs but no job name specified!')
      return job_list[0].bind(*bindings) if bindings else job_list[0]
    for job in job_list:
      if str(job.name()) == name:
        return job.bind(*bindings) if bindings else job
    raise ValueError('Could not find job named %s!' % name)

  @staticmethod
  def load(filename, name=None, bindings=None):
    tc = MesosConfigLoader()
    schema_copy = copy.copy(MesosConfigLoader.SCHEMA)
    with open(filename) as fp:
      locals_map = Compatibility.exec_function(
        compile(fp.read(), filename, 'exec'), schema_copy)
    job_list = locals_map.get('jobs')
    if not job_list or not isinstance(job_list, list):
      raise MesosConfigLoader.BadConfig("Could not extract any jobs from %s" % filename)
    return MesosConfigLoader.pick(job_list, name, bindings)

  @staticmethod
  def load_json(filename, name=None, bindings=None):
    tc = MesosConfigLoader()
    with open(filename) as fp:
      js = json.load(fp)
    job = MesosJob(js)
    return job.bind(*bindings) if bindings else job


class PystachioConfig(ProxyConfig):
  @staticmethod
  def load(filename, name=None, bindings=None):
    return PystachioConfig(MesosConfigLoader.load(filename, name, bindings))

  @staticmethod
  def load_json(filename, name=None, bindings=None):
    return PystachioConfig(MesosConfigLoader.load_json(filename, name, bindings))

  @staticmethod
  def pystachio_to_thrift(job):
    uninterpolated_vars = set()

    if not job.role().check().ok():
      raise PystachioConfig.InvalidConfig(job.role().check().message())

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
      underlying, uninterp = PystachioConfig.task_instance_from_job(job, k)
      uninterpolated_vars.update(uninterp)
      task_copy.thermosConfig = json.dumps(underlying.get())
      tasks.append(task_copy)

    cron_schedule = job.cron_schedule().get() if job.has_cron_schedule() else ''
    cron_policy = CronCollisionPolicy._NAMES_TO_VALUES.get(job.cron_policy().get(), None)
    if cron_policy is None:
      raise PystachioConfig.BadConfig('Invalid cron policy: %s' % job.cron_policy().get())

    update = UpdateConfig()
    update.batchSize = job.update_config().batch_size().get()
    update.restartThreshold = job.update_config().restart_threshold().get()
    update.watchSecs = job.update_config().watch_secs().get()
    update.maxPerShardFailures = job.update_config().max_per_shard_failures().get()
    update.maxTotalFailures = job.update_config().max_total_failures().get()

    # NOTE: Un-interpolated vars are dangerous only when they are not going
    # to be provided by the thermos context!
    if not job.check().ok():
      raise PystachioConfig.InvalidConfig(
        'Invalid Job Config: %s' % job.check().message())

    return JobConfiguration(
      str(job.name()),
      owner,
      tasks,
      cron_schedule,
      cron_policy,
      update)

  @staticmethod
  def task_instance_from_job(job, instance):
    context = MesosContext(role=job.role(),
                           instance=instance)
    ti = MesosTaskInstance(task=job.task(),
                           layout=job.layout(),
                           role=job.role(),
                           instance=instance) % Environment(mesos=context)
    return ti.interpolate()

  def __init__(self, job):
    self._job = job

  def job(self):
    return PystachioConfig.pystachio_to_thrift(self._job)

  def name(self):
    return str(self._job.name())

  def hdfs_path(self):
    return None

  def role(self):
    return self._job.role().get()

  def cluster(self):
    if self._job.cluster().check().ok():
      return self._job.cluster().get()
    else:
      return None

  def ports(self):
    return ThermosTaskWrapper(self._job.task(), strict=False).ports()
