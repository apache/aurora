import copy
import exceptions
import getpass
import sys

from twitter.common import log
from twitter.mesos.mesos_configuration import MesosConfiguration
from gen.twitter.mesos.ttypes import (
  Constraint,
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  LimitConstraint,
  ListConstraint,
  TaskConstraint,
  TwitterTaskInfo,
  UpdateConfig,
  Value,
  ValueConstraint,
)
from twitter.tcl.loader import MesosJobLoader, ThermosJobLoader
from gen.twitter.tcl.ttypes import ThermosJob, ThermosTask, ThermosProcess


def _die(msg):
  log.fatal(msg)
  sys.exit(1)


class ProxyConfig(object):
  @staticmethod
  def from_thermos(filename):
    assert filename.endswith('.thermos'), (
      "ProxyConfig.from_thermos must be called with .thermos filename")
    return ProxyThermosConfig.from_thermos(filename)

  @staticmethod
  def from_mesos(filename):
    return ProxyMesosConfig(filename)

  @staticmethod
  def from_mesos_as_thermos(filename):
    return ProxyThermosConfig.from_mesos(filename)

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

class ProxyMesosConfig(ProxyConfig):
  def __init__(self, filename):
    self._config = MesosConfiguration(filename).config
    ProxyConfig.__init__(self)

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
        negated = constraint_value.startswith('!')
        if negated:
          constraint_value=constraint_value[1:]
        values = constraint_value.split(',')
        if len(values) > 1:
          taskConstraint.listConstraint = ListConstraint()
          taskConstraint.listConstraint.negated = negated
          taskConstraint.listConstraint.values = values
        else:
          taskConstraint.valueConstraint = ValueConstraint()
          taskConstraint.valueConstraint.negated = negated
          value = Value()
          try:
            value.intValue = int(constraint_value)
          except exceptions.ValueError:
            try:
              value.doubleValue = float(constraint_value)
            except exceptions.ValueError:
              value.stringValue = constraint_value
          taskConstraint.valueConstraint.value = value
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


class ProxyThermosConfig(ProxyConfig):
  @staticmethod
  def from_mesos(filename):
    return ProxyThermosConfig(MesosJobLoader(filename).to_thrift())

  @staticmethod
  def from_thermos(filename):
    return ProxyThermosConfig(ThermosJobLoader(filename).to_thrift())

  def __init__(self, thrift_blob):
    self._config = thrift_blob
    ProxyConfig.__init__(self)

  def job(self, name=None):
    jobname = name or self._job
    assert jobname, "Job name not supplied!"
    assert self._config.job.name == jobname, """Thermos configurations only contain one job
      and the supplied job name does not match."""
    owner = Identity(role=self._config.job.role, user=getpass.getuser())

    MB = 1024 * 1024

    tasks = set()
    for task in self._config.tasks:
      tti = TwitterTaskInfo()
      tti.thermosConfig = task
      tti.jobName = self._config.job.name
      tti.numCpus = task.footprint.cpu
      tti.ramMb = task.footprint.ram / MB
      tti.diskMb = task.footprint.disk / MB
      tti.shardId = task.replica_id
      tti.maxTaskFailures = self._config.max_task_failures
      tti.owner = owner
      tasks.add(tti)

    cron_schedule = self._config.cron_schedule if self._config.cron_schedule else None

    config = JobConfiguration(
      jobname,
      owner,
      tasks,
      cron_schedule, # cron schedule
      None, # cron collision policy
      None) # update config

    return config

  def hdfs_path(self):
    return None

  def cluster(self):
    return self._config.job.cluster
