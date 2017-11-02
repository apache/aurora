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

"""A Mesos-customized entry point to the thermos command line tool."""

from twitter.common import app
from twitter.common.log.options import LogOptions

from apache.aurora.executor.common.path_detector import MesosPathDetector
from apache.thermos.cli.common import register_path_detector
from apache.thermos.cli.main import register_commands, register_options
from apache.thermos.common.excepthook import ExceptionTerminationHandler

register_commands(app)
register_options(app)


def register_mesos_root(_, __, value, ___):
  register_path_detector(MesosPathDetector(root=value))


app.add_option(
    '--mesos-root',
    dest='mesos_root',
    type='string',
    action='callback',
    default=MesosPathDetector.DEFAULT_MESOS_ROOT,
    callback=register_mesos_root,
    help='The mesos root directory to search for Thermos executor sandboxes [default: %default]')


# register a default mesos root, since the callback will not be called if no --mesos-root specified.
register_path_detector(MesosPathDetector())


LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:INFO')

app.register_module(ExceptionTerminationHandler())
app.main()
