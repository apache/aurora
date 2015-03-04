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

import getpass

import mock

from apache.thermos.cli.commands.simplerun import simplerun


@mock.patch('apache.thermos.cli.commands.simplerun.really_run')
def test_simplerun(really_run_mock):
  options_mock = mock.Mock(
      spec_set=('root', 'user', 'name', 'task_id', 'prebound_ports', 'bindings', 'daemon'))
  options_mock.root = '/tmp/root'
  options_mock.user = getpass.getuser()
  options_mock.name = 'simple'
  options_mock.task_id = None
  options_mock.prebound_ports = []
  options_mock.bindings = {}
  options_mock.daemon = False

  simplerun(['--', 'echo', 'hello', 'world'], options_mock)

  args, kw = really_run_mock.call_args
  thermos_task, root, sandbox = args
  assert str(thermos_task.task.name()) == options_mock.name
  assert str(thermos_task.task.processes()[0].cmdline()) == 'echo hello world'
  assert root == '/tmp/root'
  assert sandbox is not None
