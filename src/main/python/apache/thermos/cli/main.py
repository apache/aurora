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

from apache.thermos.monitoring.detector import FixedPathDetector

from .common import clear_path_detectors, register_path_detector


def register_commands(app):
  from apache.thermos.cli.common import generate_usage
  from apache.thermos.cli.commands import (
      help as help_command,
      inspect as inspect_command,
      kill as kill_command,
      read as read_command,
      run as run_command,
      simplerun as simplerun_command,
      status as status_command,
      tail as tail_command,
  )

  app.register_commands_from(
      help_command,
      inspect_command,
      kill_command,
      read_command,
      run_command,
      simplerun_command,
      status_command,
      tail_command,
  )

  generate_usage()


def register_root(_, __, value, parser):
  parser.values.root = value
  register_path_detector(FixedPathDetector(value))


def register_options(app):
  from apache.thermos.common.constants import DEFAULT_CHECKPOINT_ROOT

  clear_path_detectors()
  register_path_detector(FixedPathDetector(DEFAULT_CHECKPOINT_ROOT))

  app.add_option(
      '--root',
      dest='root',
      metavar='PATH',
      type='string',
      default=DEFAULT_CHECKPOINT_ROOT,
      action='callback',
      callback=register_root,
      help="the thermos config root")
