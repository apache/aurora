import copy
import functools
import getpass
import os
import re

from twitter.mesos.proxy_config import ProxyConfig
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

class EntityParser(object):
  PORT_RE = re.compile(r'%port:(\w+)%')

  @staticmethod
  def match_ports(str):
    matcher = EntityParser.PORT_RE
    matched = matcher.findall(str)
    return set(matched)

class MesosConfig(ProxyConfig):
  @staticmethod
  def execute(config_file):
    """
      Execute the .mesos configuration "config_file" in the context of preloaded
      library functions, e.g. mesos_include.
    """
    env = {}
    deposit_stack = [os.path.dirname(config_file)]
    def ast_executor(filename):
      actual_file = os.path.join(deposit_stack[-1], filename)
      deposit_stack.append(os.path.dirname(actual_file))
      execfile(actual_file, env)
      deposit_stack.pop()
    env.update({'mesos_include': lambda filename: ast_executor(filename) })
    execfile(config_file, env)
    return env

  @staticmethod
  def constraints_to_thrift(constraints):
    result = set()
    for attribute, constraint_value in constraints.items():
      assert isinstance(attribute, basestring) and isinstance(constraint_value, basestring), (
        "Both attribute name and value in constraints must be string")
      constraint = Constraint()
      constraint.name = attribute
      task_constraint = TaskConstraint()
      if constraint_value.startswith('limit:'):
        task_constraint.limitConstraint = LimitConstraint()
        try:
          task_constraint.limitConstraint.limit = int(constraint_value.replace('limit:', ''))
        except ValueError:
          print '%s is not a valid limit value, must be integer' % constraint_value
          raise
      else:
        # Strip off the leading negation if present.
        negated = constraint_value.startswith('!')
        if negated:
          constraint_value = constraint_value[1:]
        task_constraint.value = ValueConstraint(negated, set(constraint_value.split(',')))
      constraint.constraint = task_constraint
      result.add(constraint)
    return result

  @staticmethod
  def job_to_thrift(job_dict):
    config = job_dict
    owner = Identity(role=config['role'], user=getpass.getuser())

    # Force configuration map to be all strings.
    task = TwitterTaskInfo()
    if 'constraints' in config:
      task.constraints = MesosConfig.constraints_to_thrift(config['constraints'])
    task_configuration = dict((k, str(v)) for k, v in config['task'].items())
    task.configuration = task_configuration
    task.requestedPorts = EntityParser.match_ports(config['task']['start_command'])

    # Replicate task objects to reflect number of instances.
    tasks = []
    for k in range(config['instances']):
      task_copy = copy.deepcopy(task)
      task_copy.shardId = k
      tasks.append(task_copy)

    # additional stuff
    update_config = UpdateConfig(**config['update_config']) if config.get('update_config') else None

    ccp = config.get('cron_collision_policy')
    if ccp and ccp not in CronCollisionPolicy._NAMES_TO_VALUES:
      raise MesosConfig.InvalidConfig('Invalid cron collision policy: %s' % ccp)
    else:
      ccp = CronCollisionPolicy._NAMES_TO_VALUES[ccp] if ccp else None

    return JobConfiguration(
      config['name'],
      owner,
      tasks,
      config.get('cron_schedule'),
      ccp,
      update_config)


  @staticmethod
  def fill_defaults(config):
    """Validates a configuration object.
    This will make sure that the configuration object has the appropriate
    'magic' fields defined, and are of the correct types.

    We require a job name, and a task definition dictionary
    (which is uninterpreted).  A number of instances may also be specified,
    but will default to 1.

    Returns the job configurations found in the configuration object.
    """

    DEFAULT_BATCH_SIZE = 3
    DEFAULT_RESTART_THRESHOLD = 30
    DEFAULT_WATCH_SECS = 30
    DEFAULT_MAX_PER_SHARD_FAILURE = 0
    DEFAULT_MAX_TOTAL_FAILURE = 0
    if 'jobs' not in config and isinstance(config['jobs'], list):
      raise MesosConfig.InvalidConfig(
        'Configuration must define a python list named "jobs"')

    job_dicts = config['jobs']
    jobs = {}
    has_errors = False

    for job in job_dicts:
      errors = []
      if 'name' not in job:
        errors.append('Missing required option: name')
      elif job['name'] in jobs:
        errors.append('Duplicate job definition')
      if 'owner' in job:
        errors.append("'owner' is deprecated.  Please use role.")
        if 'role' in job:
          errors.append('Ambiguous specification: both owner and role specified.')
        else:
          job['role'] = job['owner']
          del job['owner']
      else:
        if 'role' not in job:
          errors.append('Must specify role.')
      if 'task' not in job:
        errors.append('Missing required option: task')
      elif not isinstance(job['task'], dict):
        errors.append('Task configuration must be a python dictionary.')
      if 'start_command' not in job['task']:
        errors.append('Task must have start_command.')

      if errors:
        raise MesosConfig.InvalidConfig('Invalid configuration: %s\n' % '\n'.join(errors))

      # Default to a single instance.
      if 'instances' not in job:
        job['instances'] = 1

      if 'cron_schedule' not in job:
        job['cron_schedule'] = ''

      update_config = job.get('update_config', {})
      if not 'batchSize' in update_config:
        update_config['batchSize'] = DEFAULT_BATCH_SIZE
      if not 'restartThreshold' in update_config:
        update_config['restartThreshold'] = DEFAULT_RESTART_THRESHOLD
      if not 'watchSecs' in update_config:
        update_config['watchSecs'] = DEFAULT_WATCH_SECS
      if not 'maxPerShardFailures' in update_config:
        update_config['maxPerShardFailures'] = DEFAULT_MAX_PER_SHARD_FAILURE
      if not 'maxTotalFailures' in update_config:
        update_config['maxTotalFailures'] = DEFAULT_MAX_TOTAL_FAILURE

      jobs[job['name']] = job
    return jobs


  def __init__(self, filename, name=None):
    """Loads a job configuration from a file and validates it.

    Returns a validated configuration object.
    """
    env = MesosConfig.execute(filename)
    self._config = MesosConfig.fill_defaults(env)
    if name is None:
      if len(self._config) == 1:
        self._config = self._config.values()[0]
      else:
        raise ValueError('Must specify job name in multi-job configuration!')
    else:
      if name not in self._config:
        raise ValueError('Unknown job %s!' % name)
      self._config = self._config[name]
    self._thrift_config = MesosConfig.job_to_thrift(self._config)
    self._name = self._config['name']

  def job(self):
    return self._thrift_config

  def ports(self):
    return EntityParser.match_ports(self._config['task']['start_command'])

  def hdfs_path(self):
    return self._config['task'].get('hdfs_path')

  def role(self):
    return self._config['role']

  def cluster(self):
    return self._config.get('cluster')

  def name(self):
    return self._name
