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

import pwd
from collections import namedtuple
from contextlib import closing

from twitter.common import log
from twitter.common.recordio import RecordIO, ThriftRecordReader

from apache.thermos.common.ckpt import CheckpointDispatcher
from apache.thermos.common.path import TaskPath

from .muxer import ProcessMuxer

from gen.apache.thermos.ttypes import ProcessState, RunnerCkpt, RunnerState

CheckpointInspection = namedtuple('CheckpointInspection',
    ['runner_latest_update',
     'process_latest_update',
     'runner_processes',
     'coordinator_processes',
     'processes'])


class CheckpointInspector(object):
  def __init__(self, checkpoint_root):
    self._path = TaskPath(root=checkpoint_root)

  @staticmethod
  def get_timestamp(process_record):
    if process_record:
      for timestamp in ('fork_time', 'start_time', 'stop_time'):
        stamp = getattr(process_record, timestamp, None)
        if stamp:
          return stamp
    return 0

  def inspect(self, task_id):
    """
      Reconstructs the checkpoint stream and returns a CheckpointInspection.
    """
    dispatcher = CheckpointDispatcher()
    state = RunnerState(processes={})
    muxer = ProcessMuxer(self._path.given(task_id=task_id))

    runner_processes = []
    coordinator_processes = set()
    processes = set()

    def consume_process_record(record):
      if not record.process_status:
        return
      try:
        user_uid = pwd.getpwnam(state.header.user).pw_uid
      except KeyError:
        log.error('Could not find user: %s' % state.header.user)
        return
      if record.process_status.state == ProcessState.FORKED:
        coordinator_processes.add((record.process_status.coordinator_pid, user_uid,
                                   record.process_status.fork_time))
      elif record.process_status.state == ProcessState.RUNNING:
        processes.add((record.process_status.pid, user_uid,
                       record.process_status.start_time))

    # replay runner checkpoint
    runner_pid = None
    runner_latest_update = 0
    try:
      with open(self._path.given(task_id=task_id).getpath('runner_checkpoint')) as fp:
        with closing(ThriftRecordReader(fp, RunnerCkpt)) as ckpt:
          for record in ckpt:
            dispatcher.dispatch(state, record)
            runner_latest_update = max(runner_latest_update,
                self.get_timestamp(record.process_status))
            # collect all bound runners
            if record.task_status:
              if record.task_status.runner_pid != runner_pid:
                runner_processes.append((record.task_status.runner_pid,
                                         record.task_status.runner_uid or 0,
                                         record.task_status.timestamp_ms))
                runner_pid = record.task_status.runner_pid
            elif record.process_status:
              consume_process_record(record)
    except (IOError, OSError, RecordIO.Error) as err:
      log.debug('Error inspecting task runner checkpoint: %s' % err)
      return

    # register existing processes in muxer
    for process_name in state.processes:
      muxer.register(process_name)

    # read process checkpoints
    process_latest_update = runner_latest_update
    for record in muxer.select():
      process_latest_update = max(process_latest_update, self.get_timestamp(record.process_status))
      consume_process_record(record)

    return CheckpointInspection(
      runner_latest_update=runner_latest_update,
      process_latest_update=process_latest_update,
      runner_processes=runner_processes,
      coordinator_processes=coordinator_processes,
      processes=processes)
