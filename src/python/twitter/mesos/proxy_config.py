import copy
import exceptions
import getpass
import sys

from twitter.common import log
from twitter.mesos.config.loader import MesosConfigLoader
from twitter.mesos.mesos_configuration import MesosConfiguration
from gen.twitter.mesos.ttypes import (
  Constraint,
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  LimitConstraint,
  TaskConstraint,
  TwitterTaskInfo,
  UpdateConfig,
  ValueConstraint,
)

def _die(msg):
  log.fatal(msg)
  sys.exit(1)


class ProxyConfig(object):
  @staticmethod
  def from_new_mesos(filename):
    return ProxyNewMesosConfig.load_file(filename)

  @staticmethod
  def from_new_mesos_json(filename):
    return ProxyNewMesosConfig.load_json(filename)


  @staticmethod
  def from_mesos(filename):
    return ProxyMesosConfig(filename)

  def __init__(self):
    self._job = None

  def set_job(self, job):
    self._job = job

  def name(self):
    return self._job

  def hdfs_path(self):
    raise NotImplementedError

  def cluster(self):
    raise NotImplementedError

  def ports(self):
    return set()


class ProxyMesosConfig(ProxyConfig):
  def __init__(self, filename):
    self._mesos_config = MesosConfiguration(filename)
    self._config = self._mesos_config.config
    ProxyConfig.__init__(self)

  def ports(self):
    return self._mesos_config.ports(self.name())

  @staticmethod
  def parse_constraints(constraints_dict):
    result = set()
    for attribute, constraint_value in constraints_dict.items():
      assert isinstance(attribute, basestring) and isinstance(constraint_value, basestring), (
        "Both attribute name and value in constraints must be string")
      constraint = Constraint()
      constraint.attribute = attribute
      taskConstraint = TaskConstraint()
      if constraint_value.startswith('limit:'):
        taskConstraint.limitConstraint = LimitConstraint()
        try:
          taskConstraint.limitConstraint.limit = int(constraint_value.replace('limit:', ''))
        except ValueError:
          print '%s is not a valid limit value, must be integer' % constraint_value
          raise
      else:
        # Strip off the leading negation if present.
        negated = constraint_value.startswith('!')
        if negated:
          constraint_value = constraint_value[1:]
        taskConstraint.value = ValueConstraint(negated, set(constraint_value.split(',')))
      constraint.constraint = taskConstraint
      result.add(constraint)

    return result

  def job(self, name=None):
    jobname = name or self._job
    assert jobname, "Job name not supplied!"

    config = self._config[jobname]

    if 'role' not in config:
      _die('role must be specified!')
    owner = Identity(role=config['role'], user=getpass.getuser())

    # Force configuration map to be all strings.
    task = TwitterTaskInfo()
    task_constraints = {}
    if 'constraints' in config:
      task_constraints['constraints'] = ProxyMesosConfig.parse_constraints(config['constraints'])
      del config['constraints']
    task_configuration = dict((k, str(v)) for k, v in config['task'].items())
    task.configuration = dict(task_configuration.items() + task_constraints.items())
    task.requestedPorts = self.ports()

    # Replicate task objects to reflect number of instances.
    tasks = []
    for k in range(config['instances']):
      taskCopy = copy.deepcopy(task)
      taskCopy.shardId = k
      tasks.append(taskCopy)

    # additional stuff
    update_config_params = (
      'batchSize', 'restartThreshold', 'watchSecs', 'maxPerShardFailures', 'maxTotalFailures')
    update_config = UpdateConfig(
      *map(lambda attribute: config['update_config'].get(attribute), update_config_params))
    ccp = config.get('cron_collision_policy')
    if ccp and ccp not in CronCollisionPolicy._NAMES_TO_VALUES:
      _die('Invalid cron collision policy: %s' % ccp)
    else:
      ccp = CronCollisionPolicy._NAMES_TO_VALUES[ccp] if ccp else None

    return JobConfiguration(
      config['name'],
      owner,
      tasks,
      config.get('cron_schedule'),
      ccp,
      update_config)

  def hdfs_path(self, name=None):
    jobname = name or self._job
    assert jobname, "Job name must be specified!"
    return self._config[jobname]['task'].get('hdfs_path', None)

  def cluster(self):
    return self._config.get('cluster')


class ProxyNewMesosConfig(ProxyConfig):
  @staticmethod
  def load_file(filename):
    return ProxyNewMesosConfig(MesosConfigLoader.load(filename))

  @staticmethod
  def load_json(filename):
    return ProxyNewMesosConfig(MesosConfigLoader.load_json(filename))

  def __init__(self, config):
    self._config = config
    ProxyConfig.__init__(self)

  def _get_wrap_job(self, name=None):
    """Returns the wrapped job."""
    jobname = name or self._job
    assert jobname, "Job name not supplied!"

    job_wrap = self._config.job(jobname)
    assert job_wrap, "Job %s is not specified in the config file." % jobname

    return job_wrap

  def job(self, name=None):
    """Returns the mesos thrift job configuration."""
    job_wrap = self._get_wrap_job(name)
    job_raw = job_wrap.job()

    if not job_raw.role().check().ok():
      _die('role must be specified!')

    owner = Identity(role=job_raw.role().get(), user=getpass.getuser())

    task_wrap = job_wrap.task()
    task_raw = task_wrap.task()

    MB = 1024 * 1024

    task = TwitterTaskInfo()
    task.jobName = task_raw.name().get()
    task.numCpus = task_raw.resources().cpu().get()
    task.ramMb = task_raw.resources().ram().get() / MB
    task.diskMb = task_raw.resources().disk().get() / MB
    task.maxTaskFailures = task_raw.max_failures().get()
    task.owner = owner
    task.requestedPorts = set(task_wrap.ports())

    # Replicate task objects to reflect number of instances.
    tasks = []
    for k in range(job_raw.instances().get()):
      taskCopy = copy.deepcopy(task)
      taskCopy.shardId = k
      taskCopy.thermosConfig = job_wrap.task_instance_json(k)
      tasks.append(taskCopy)

    cron_schedule = job_raw.cron_schedule().get() if job_raw.has_cron_schedule() else ''

    cron_policy = CronCollisionPolicy._NAMES_TO_VALUES.get(job_raw.cron_policy().get(), None)
    assert cron_policy is not None, "Invalid cron policy: %s" % job_raw.cron_policy().get()

    update = UpdateConfig()
    update.batchSize = job_raw.update_config().batch_size().get()
    update.restartThreshold = job_raw.update_config().restart_threshold().get()
    update.watchSecs = job_raw.update_config().watch_secs().get()
    update.maxPerShardFailures = job_raw.update_config().failures_per_shard().get()
    update.maxTotalFailures = job_raw.update_config().total_failures().get()

    return JobConfiguration(
      job_raw.name().get(),
      owner,
      tasks,
      cron_schedule,
      cron_policy,
      update)

  def hdfs_path(self):
    return None

  def cluster(self):
    job_raw = self._get_wrap_job().job()

    if job_raw.cluster().check().ok():
      return job_raw.cluster().get()
    else:
      return None
