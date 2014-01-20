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


BIND_OPTION = CommandOption('--bind', type=str, default=[], dest='bindings',
    action='append',
    help='Bind a thermos mustache variable name to a value. '
    'Multiple flags may be used to specify multiple values.')


BROWSER_OPTION = CommandOption('--open-browser', default=False, dest='open_browser',
    action='store_true',
    help='open browser to view job page after job is created')


CONFIG_ARGUMENT = CommandOption('config_file', type=str,
    help='pathname of the aurora configuration file contain the job specification')


JOBSPEC_ARGUMENT = CommandOption('jobspec', type=AuroraJobKey.from_path,
    help='Fully specified job key, in CLUSTER/ROLE/ENV/NAME format')


JSON_OPTION = CommandOption('--json', default=False, dest='json', action='store_true',
    help='Read job configuration in json format')
