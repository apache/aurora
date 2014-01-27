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

from apache.aurora.client.cli import CommandOption
from apache.aurora.common.aurora_job_key import AuroraJobKey


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


WATCH_OPTION = CommandOption('--watch_secs', type=int, default=30,
    help='Minimum number of seconds a shard must remain in RUNNING state before considered a '
         'success.')
