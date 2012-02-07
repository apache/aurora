import copy
import getpass
import json
import sys

from pystachio import Ref, Environment
from twitter.common.dirutil import safe_open
from twitter.common.lang.compatibility import *
from twitter.mesos.config.schema import (
  MesosJob,
  MesosTaskInstance,
  MesosContext)
from twitter.thermos.config.loader import ThermosTaskWrapper
from twitter.mesos.proxy_config import ProxyConfig
from gen.twitter.mesos.ttypes import (
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  TwitterTaskInfo,
  UpdateConfig,
)


SCHEMA_PREAMBLE = """
from pystachio import *
from twitter.mesos.config.schema import *
from twitter.thermos.config.dsl import *
"""

def deposit_schema(environment):
  exec_function(compile(SCHEMA_PREAMBLE, "<exec_function>", "exec"), environment)

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

  @staticmethod
  def load(filename, name=None, bindings=None):
    tc = MesosConfigLoader()
    schema_copy = copy.copy(MesosConfigLoader.SCHEMA)
    with open(filename) as fp:
      locals_map = exec_function(compile(fp.read(), filename, 'exec'), schema_copy)
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
    task.jobName = task_raw.name().get()
    task.numCpus = task_raw.resources().cpu().get()
    task.ramMb = task_raw.resources().ram().get() / MB
    task.diskMb = task_raw.resources().disk().get() / MB
    task.maxTaskFailures = task_raw.max_failures().get()
    task.owner = owner
    task.requestedPorts = ThermosTaskWrapper(task_raw, strict=False).ports()

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
    update.maxPerShardFailures = job.update_config().failures_per_shard().get()
    update.maxTotalFailures = job.update_config().total_failures().get()

    if uninterpolated_vars:
      raise PystachioConfig.InvalidConfig(
        'Missing environment bindings:\n%s' % '\n'.join(map(str, uninterpolated_vars)))

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
                           instance=instance) % Environment(mesos = context)
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
      return job.cluster().get()
    else:
      return None

  def ports(self):
    return ThermosTaskWrapper(self._job.task(), strict=False).ports()
