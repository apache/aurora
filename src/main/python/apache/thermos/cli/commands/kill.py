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

from __future__ import print_function

import sys

from twitter.common import app

from apache.thermos.cli.common import tasks_from_re
from apache.thermos.core.helper import TaskRunnerHelper


@app.command
def kill(args, options):
  """Kill task(s)

  Usage: thermos kill task_id1 [task_id2 ...]

  Regular expressions may be used to match multiple tasks.
  """
  if not args:
    print('Must specify tasks!', file=sys.stderr)
    return

  matched_tasks = tasks_from_re(args, state='active')

  if not matched_tasks:
    print('No active tasks matched.')
    return

  for root, task_id in matched_tasks:
    print('Killing %s...' % task_id, end='')
    TaskRunnerHelper.kill(task_id, root, force=True)
    print('done.')
