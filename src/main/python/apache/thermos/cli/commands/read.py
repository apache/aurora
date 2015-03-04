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
import time

from twitter.common import app
from twitter.common.recordio import RecordIO, ThriftRecordReader

from apache.thermos.common.ckpt import CheckpointDispatcher

from gen.apache.thermos.ttypes import ProcessState, RunnerCkpt, RunnerState, TaskState


@app.command
@app.command_option("--simple", default=False, dest='simple', action='store_true',
                    help="Only print the checkpoint records, do not replay them.")
def read(args, options):
  """Replay a thermos checkpoint.

  Usage: thermos read [options] checkpoint_filename
  """
  if len(args) != 1:
    app.error('Expected one checkpoint file, got %s' % len(args))
  if not os.path.exists(args[0]):
    app.error('Could not find %s' % args[0])

  dispatcher = CheckpointDispatcher()
  state = RunnerState(processes={})
  with open(args[0], 'r') as fp:
    try:
      for record in ThriftRecordReader(fp, RunnerCkpt):
        if not options.simple:
          dispatcher.dispatch(state, record)
        else:
          print('CKPT: %s' % record)
    except RecordIO.Error as err:
      print("Failed to recover from %s: %s" % (fp.name, err))
      return

  if not options.simple:
    if state is None or state.header is None:
      print('Checkpoint stream CORRUPT or outdated format')
      return
    print('Recovered Task Header:')
    print('  id:      %s' % state.header.task_id)
    print('  user:    %s' % state.header.user)
    print('  host:    %s' % state.header.hostname)
    print('  sandbox: %s' % state.header.sandbox)
    if state.header.ports:
      print('  ports:   %s' % ' '.join(
        '%s->%s' % (name, port) for (name, port) in state.header.ports.items()))
    print('Recovered Task States:')
    for task_status in state.statuses:
      print('  %s [pid: %d] => %s' % (
        time.asctime(time.localtime(task_status.timestamp_ms / 1000.0)),
        task_status.runner_pid,
        TaskState._VALUES_TO_NAMES[task_status.state]))
    print('Recovered Processes:')
    for process, process_history in state.processes.items():
      print('  %s   runs: %s' % (process, len(process_history)))
      for k in reversed(range(len(process_history))):
        run = process_history[k]
        print('    %2d: pid=%d, rc=%s, finish:%s, state:%s' % (
          k,
          run.pid,
          run.return_code if run.return_code is not None else '',
          time.asctime(time.localtime(run.stop_time)) if run.stop_time else 'None',
          ProcessState._VALUES_TO_NAMES.get(run.state, 'Unknown')))
