#
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

import copy
import json
import pprint
import sys

from twitter.common import app

from apache.thermos.config.loader import ThermosConfigLoader


def main(args):
  """
    Given .thermos configs, loads them and prints out information about them.
  """

  if len(args) == 0:
    app.help()

  for arg in args:
    print '\nparsing %s\n' % arg
    tc = ThermosConfigLoader.load(arg)

    for task_wrapper in tc.tasks():
      task = task_wrapper.task
      if not task.has_name():
        print 'Found unnamed task!  Skipping...'
        continue

      print 'Task: %s [check: %s]' % (task.name(), task.check())
      if not task.processes():
        print '  No processes.'
      else:
        print '  Processes:'
        for proc in task.processes():
          print '    %s' % proc

      ports = task_wrapper.ports()
      if not ports:
        print '  No unbound ports.'
      else:
        print '  Ports:'
        for port in ports:
          print '    %s' % port

      print 'raw:'
      pprint.pprint(json.loads(task_wrapper.to_json()))

app.set_usage("%s config1 config2 ..." % app.name())
app.main()
