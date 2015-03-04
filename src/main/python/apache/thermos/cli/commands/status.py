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

import os
import pwd
import re
import sys
import time

from twitter.common import app

from apache.thermos.cli.common import get_path_detector
from apache.thermos.common.ckpt import CheckpointDispatcher
from apache.thermos.monitoring.detector import TaskDetector

from gen.apache.thermos.ttypes import ProcessState, TaskState


@app.command
@app.command_option("--verbosity", default=0, dest='verbose', type='int',
                    help="Display more verbosity")
@app.command_option("--only", default=None, dest='only', type='choice',
                    choices=('active', 'finished'), help="Display only tasks of this type.")
def status(args, options):
  """Get the status of task(s).

    Usage: thermos status [options] [task_name(s) or task_regexp(s)]
  """
  path_detector = get_path_detector()

  def format_task(detector, task_id):
    checkpoint_filename = detector.get_checkpoint(task_id)
    checkpoint_stat = os.stat(checkpoint_filename)
    try:
      checkpoint_owner = pwd.getpwuid(checkpoint_stat.st_uid).pw_name
    except KeyError:
      checkpoint_owner = 'uid:%s' % checkpoint_stat.st_uid
    print('  %-20s [owner: %8s]' % (task_id, checkpoint_owner), end='')
    if options.verbose == 0:
      print()
    if options.verbose > 0:
      state = CheckpointDispatcher.from_file(checkpoint_filename)
      if state is None or state.header is None:
        print(' - checkpoint stream CORRUPT or outdated format')
        return
      print('  state: %8s' % TaskState._VALUES_TO_NAMES.get(state.statuses[-1].state, 'Unknown'),
        end='')
      print(' start: %25s' % time.asctime(time.localtime(state.header.launch_time_ms / 1000.0)))
    if options.verbose > 1:
      print('    user: %s' % state.header.user, end='')
      if state.header.ports:
        print(' ports: %s' % ' '.join('%s -> %s' % (key, val)
                                         for key, val in state.header.ports.items()))
      else:
        print(' ports: None')
      print('    sandbox: %s' % state.header.sandbox)
    if options.verbose > 2:
      print('    process table:')
      for process, process_history in state.processes.items():
        print('      - %s runs: %s' % (process, len(process_history)), end='')
        last_run = process_history[-1]
        print(' last: pid=%s, rc=%s, finish:%s, state:%s' % (
          last_run.pid or 'None',
          last_run.return_code if last_run.return_code is not None else '',
          time.asctime(time.localtime(last_run.stop_time)) if last_run.stop_time else 'None',
          ProcessState._VALUES_TO_NAMES.get(last_run.state, 'Unknown')))
      print()

  matchers = map(re.compile, args or ['.*'])

  active = []
  finished = []

  for root in path_detector.get_paths():
    detector = TaskDetector(root)
    active.extend((detector, t_id) for _, t_id in detector.get_task_ids(state='active')
        if any(pattern.match(t_id) for pattern in matchers))
    finished.extend((detector, t_id)for _, t_id in detector.get_task_ids(state='finished')
        if any(pattern.match(t_id) for pattern in matchers))

  found = False
  if options.only is None or options.only == 'active':
    if active:
      print('Active tasks:')
      found = True
      for detector, task_id in active:
        format_task(detector, task_id)
      print()

  if options.only is None or options.only == 'finished':
    if finished:
      print('Finished tasks:')
      found = True
      for detector, task_id in finished:
        format_task(detector, task_id)
      print()

  if not found:
    print('No tasks found.')
    sys.exit(1)
