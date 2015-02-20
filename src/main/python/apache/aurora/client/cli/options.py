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

from argparse import ArgumentTypeError
from collections import namedtuple

from pystachio import Ref
from twitter.common.quantity.parse_simple import parse_time

from apache.aurora.common.aurora_job_key import AuroraJobKey


# TODO(wfarner): Consider removing, it doesn't appear this wrapper is useful.
class CommandOption(object):
  """A lightweight encapsulation of an argparse option specification"""

  def __init__(self, *args, **kwargs):
    # Search for the long-name: it's either the first argument that
    # is a double-dashed option name, or the first argument that doesn't
    # have a dash in front of it.
    for arg in args:
      if arg.startswith('--') or not arg.startswith('-'):
        self.name = arg
        break
    else:
      raise ValueError('CommandOption had no valid name.')
    self.args = args[:]
    self.kwargs = kwargs.copy()
    self.type = self.kwargs.get('type')
    self.help = self.kwargs.get('help', '')

  def add_to_parser(self, parser):
    """Add this option to an option parser"""
    parser.add_argument(*self.args, **self.kwargs)


def parse_qualified_role(rolestr):
  if rolestr is None:
    raise ArgumentTypeError('Role argument cannot be empty!')
  role_parts = rolestr.split('/')
  if len(role_parts) != 2:
    raise ArgumentTypeError('Role argument must be a CLUSTER/NAME pair')
  return role_parts


ALL_INSTANCES = None


def parse_instances(instances):
  """Parse lists of instances or instance ranges into a set(). This accepts a comma-separated
  list of instances.

     Examples:
       0-2
       0,1-3,5
       1,3,5
  """
  if instances is None or instances == "":
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
  try:
    return sorted(map(parse_time, time_values.split(','))) if time_values else None
  except ValueError as e:
    raise ArgumentTypeError(e)


def parse_percentiles(percentiles):
  """Parse lists of percentile values in (0,100) range.
     Examples:
       .2
       10,15,99.9
  """
  def parse_percentile(val):
    try:
      result = float(val)
      if result <= 0 or result >= 100:
        raise ValueError()
    except ValueError:
      raise ArgumentTypeError('Invalid percentile value:%s. '
                              'Must be between 0.0 and 100.0 exclusive.' % val)
    return result

  return sorted(map(parse_percentile, percentiles.split(','))) if percentiles else None


TaskInstanceKey = namedtuple('TaskInstanceKey', ['jobkey', 'instance'])


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


def instance_specifier(spec_str):
  if spec_str is None or spec_str == '':
    raise ValueError('Instance specifier must be non-empty')
  parts = spec_str.split('/')
  if len(parts) == 4:
    jobkey = AuroraJobKey(*parts)
    return TaskInstanceKey(jobkey, ALL_INSTANCES)
  elif len(parts) != 5:
    raise ArgumentTypeError('Instance specifier must be a CLUSTER/ROLE/ENV/JOB/INSTANCES tuple')
  (cluster, role, env, name, instance_str) = parts
  jobkey = AuroraJobKey(cluster, role, env, name)
  instances = parse_instances(instance_str)
  return TaskInstanceKey(jobkey, instances)


def binding_parser(binding):
  """Pystachio takes bindings in the form of a list of dictionaries. Each pystachio binding
  becomes another dictionary in the list. So we need to convert the bindings specified by
  the user from a list of "name=value" formatted strings to a list of the dictionaries
  expected by pystachio.
  """
  binding_parts = binding.split("=")
  if len(binding_parts) != 2:
    raise ValueError('Binding parameter must be formatted name=value')
  try:
    ref = Ref.from_address(binding_parts[0])
  except Ref.InvalidRefError as e:
    raise ValueError("Could not parse binding parameter %s: %s" % (binding, e))
  return {ref: binding_parts[1]}


BATCH_OPTION = CommandOption('--batch-size', type=int, default=1,
        help='Number of instances to be operate on in one iteration')


BIND_OPTION = CommandOption('--bind', dest='bindings',
    action='append',
    default=[],
    metavar="var=value",
    type=binding_parser,
    help='Bind a pystachio variable name to a value. '
    'Multiple flags may be used to specify multiple values.')


BROWSER_OPTION = CommandOption('--open-browser', default=False, dest='open_browser',
    action='store_true',
    help='open browser to view job page after job is created')


CONFIG_ARGUMENT = CommandOption('config_file', type=str, metavar="pathname",
    help='pathname of the aurora configuration file contain the job specification')

CONFIG_OPTION = CommandOption('--config', type=str, default=None, metavar="pathname",
    help='pathname of the aurora configuration file contain the job specification')

EXECUTOR_SANDBOX_OPTION = CommandOption('--executor-sandbox', action='store_true',
     default=False, help='Run the command in the executor sandbox instead of the task sandbox')


FORCE_OPTION = CommandOption('--force', default=False, action='store_true',
    help='Force execution of the command even if there is a warning')


HEALTHCHECK_OPTION = CommandOption('--healthcheck-interval-seconds', type=int,
    default=3, dest='healthcheck_interval_seconds',
    help='Number of seconds between healthchecks while monitoring update')


INSTANCES_OPTION = CommandOption('--instances', type=parse_instances, dest='instances',
    default=None, metavar="inst,inst,inst...",
     help='A list of instance ids to act on. Can either be a comma-separated list (e.g. 0,1,2) '
         'or a range (e.g. 0-2) or any combination of the two (e.g. 0-2,5,7-9). If not set, '
         'all instances will be acted on.')

INSTANCES_SPEC_ARGUMENT = CommandOption('instance_spec', type=instance_specifier,
    default=None, metavar="CLUSTER/ROLE/ENV/NAME[/INSTANCES]",
    help=('Fully specified job instance key, in CLUSTER/ROLE/ENV/NAME[/INSTANCES] format. '
        'If INSTANCES is omitted, then all instances will be operated on.'))


def jobkeytype(v):
  """wrapper for AuroraJobKey.from_path that improves error messages"""
  return AuroraJobKey.from_path(v)


JOBSPEC_ARGUMENT = CommandOption('jobspec', type=jobkeytype,
    metavar="CLUSTER/ROLE/ENV/NAME",
    help='Fully specified job key, in CLUSTER/ROLE/ENV/NAME format')


JOBSPEC_OPTION = CommandOption('--job', type=AuroraJobKey.from_path,
    metavar="CLUSTER/ROLE/ENV/NAME",
    dest="jobspec",
    help='Fully specified job key, in CLUSTER/ROLE/ENV/NAME format')


JSON_READ_OPTION = CommandOption('--read-json', default=False, dest='read_json',
    action='store_true',
    help='Read job configuration in json format')


JSON_WRITE_OPTION = CommandOption('--write-json', default=False, dest='write_json',
    action='store_true',
    help='Generate command output in JSON format')


MAX_TOTAL_FAILURES_OPTION = CommandOption('--max-total-failures', type=int, default=0,
     help='Maximum number of instance failures to be tolerated in total before aborting.')


NO_BATCHING_OPTION = CommandOption('--no-batching', default=False, action='store_true',
  help='Run the command on all instances at once, instead of running in batches')


ROLE_ARGUMENT = CommandOption('role', type=parse_qualified_role, metavar='CLUSTER/NAME',
    help='Rolename to retrieve information about')

ROLE_OPTION = CommandOption('--role', metavar='ROLENAME', default=None,
    help='Name of the user/role')

SSH_USER_OPTION = CommandOption('--ssh-user', '-l', default=None, metavar="ssh_username",
    help='ssh as this username instead of the job\'s role')


STRICT_OPTION = CommandOption('--strict', default=False, action='store_true',
    help=("Check instances and generate an error for instance ranges in parameters "
    "that are larger than the actual set of instances in the job"))


TASK_INSTANCE_ARGUMENT = CommandOption('task_instance', type=parse_task_instance_key,
    help='A task instance specifier, in the form CLUSTER/ROLE/ENV/NAME/INSTANCE')


WATCH_OPTION = CommandOption('--watch-secs', type=int, default=30,
    help='Minimum number of seconds a instance must remain in RUNNING state before considered a '
         'success.')
