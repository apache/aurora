import copy
import getpass

from gen.twitter.mesos.ttypes import (
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  TwitterTaskInfo,
)

from .base import EntityParser, ThriftCodec

def convert(config):
  """
    Convert a Mesos job dictionary (config) to Thrift.  The dictionary
    should be validated and have its defaults filled.
  """

  owner = Identity(role=config['role'], user=getpass.getuser())

  # Force configuration map to be all strings.
  task = TwitterTaskInfo()
  if 'constraints' in config:
    task.constraints = ThriftCodec.constraints_to_thrift(config['constraints'])
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
  ccp = config.get('cron_collision_policy')
  if ccp and ccp not in CronCollisionPolicy._NAMES_TO_VALUES:
    raise ValueError('Invalid cron collision policy: %s' % ccp)
  else:
    ccp = CronCollisionPolicy._NAMES_TO_VALUES[ccp] if ccp else None

  return JobConfiguration(
    config['name'],
    owner,
    tasks,
    config.get('cron_schedule'),
    ccp)
