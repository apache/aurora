#
# Copyright 2013 Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from collections import namedtuple

from apache.aurora.client.cli import CommandOption
from apache.aurora.common.aurora_job_key import AuroraJobKey

from twitter.common.quantity.parse_simple import parse_time


def parse_qualified_role(rolestr):
  if rolestr is None:
    raise ValueError('Role argument cannot be empty!')
  role_parts = rolestr.split('/')
  if len(role_parts) != 2:
    raise ValueError('Role argument must be a CLUSTER/NAME pair')
  return role_parts


def parse_instances(instances):
  """Parse lists of instances or instance ranges into a set().
     Examples:
       0-2
       0,1-3,5
       1,3,5
  """
  if instances is None or instances == '':
    return None
  result = set()
  for part in instances.split(','):
    x = part.split('-')
    result.update(range(int(x[0]), int(x[-1]) + 1))
  return sorted(result)

def parse_time_values(time_values):
  """Parse lists of discrete time values. Every value must be in the following format: XdYhZmWs.
     Examples:
       15m
       1m,1d,3h25m,2h4m15s
  """
  if time_values is None or time_values == '':
    return None
  return sorted(map(parse_time, time_values.split(',')))


TaskInstanceKey = namedtuple('TaskInstanceKey', [ 'jobkey', 'instance' ])

def parse_task_instance_key(key):
  pieces = key.split('/')
  if len(pieces) != 5:
    raise ValueError('Task instance specifier %s is not in the form '
        'CLUSTER/ROLE/ENV/NAME/INSTANCE' % key)
  (cluster, role, env, name, instance_str) = pieces
  try:
    instance = int(instance_str)
  except ValueError:
    raise ValueError('Instance must be an integer, but got %s' % instance_str)
  return TaskInstanceKey(AuroraJobKey(cluster, role, env, name), instance)


BATCH_OPTION = CommandOption('--batch_size', type=int, default=5,
        help='Number of instances to be operate on in one iteration')


BIND_OPTION = CommandOption('--bind', type=str, default=[], dest='bindings',
    action='append',
    help='Bind a thermos mustache variable name to a value. '
    'Multiple flags may be used to specify multiple values.')


BROWSER_OPTION = CommandOption('--open-browser', default=False, dest='open_browser',
    action='store_true',
    help='open browser to view job page after job is created')


CONFIG_ARGUMENT = CommandOption('config_file', type=str,
    help='pathname of the aurora configuration file contain the job specification')


EXECUTOR_SANDBOX_OPTION = CommandOption('--executor_sandbox', action='store_true',
     default=False, help='Run the command in the executor sandbox instead of the task sandbox')


FORCE_OPTION = CommandOption('--force', default=False, action='store_true',
    help='Force execution of the command even if there is a warning')


HEALTHCHECK_OPTION = CommandOption('--healthcheck_interval_seconds', type=int,
    default=3, dest='healthcheck_interval_seconds',
    help='Number of seconds between healthchecks while monitoring update')


INSTANCES_OPTION = CommandOption('--instances', type=parse_instances, dest='instances',
    default=None,
     help='A list of instance ids to act on. Can either be a comma-separated list (e.g. 0,1,2) '
         'or a range (e.g. 0-2) or any combination of the two (e.g. 0-2,5,7-9). If not set, '
         'all instances will be acted on.')


JOBSPEC_ARGUMENT = CommandOption('jobspec', type=AuroraJobKey.from_path,
    help='Fully specified job key, in CLUSTER/ROLE/ENV/NAME format')


JSON_READ_OPTION = CommandOption('--read_json', default=False, dest='read_json',
    action='store_true',
    help='Read job configuration in json format')


JSON_WRITE_OPTION = CommandOption('--write_json', default=False, dest='write_json',
    action='store_true',
    help='Generate command output in JSON format')


ROLE_ARGUMENT = CommandOption('role', type=parse_qualified_role,
    help='Rolename to retrieve information about, in CLUSTER/NAME format')


SSH_USER_OPTION = CommandOption('--ssh_user', '-l', default=None,
    help='ssh as this username instead of the job\'s role')


TASK_INSTANCE_ARGUMENT = CommandOption('task_instance', type=parse_task_instance_key,
    help='A task instance specifier, in the form CLUSTER/ROLE/ENV/NAME/INSTANCE')


WATCH_OPTION = CommandOption('--watch_secs', type=int, default=30,
    help='Minimum number of seconds a instance must remain in RUNNING state before considered a '
         'success.')



