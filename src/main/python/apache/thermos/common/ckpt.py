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

"""Read checkpoint streams for the Thermos runner, and dispatch events on state transitions

This module contains the CheckpointDispatcher, which reconstructs checkpoint streams containing the
state of the Thermos runner and its constituent processes.

It also defines several Handler interfaces to define behaviour on transitions in the Process and
Task state machines.

"""

import os

from twitter.common import log
from twitter.common.recordio import RecordIO, ThriftRecordReader

from gen.apache.thermos.ttypes import (
    ProcessState,
    ProcessStatus,
    RunnerCkpt,
    RunnerState,
    TaskState
)


class UniversalStateHandler(object):
  """
    Generic interface for a handler to be called on any process/state transition, and at task
    initialization
  """
  def on_process_transition(self, state, process_update):
    pass

  def on_task_transition(self, state, task_update):
    pass

  def on_initialization(self, header):
    pass


class ProcessStateHandler(object):
  """
    Interface for handlers for the Process state machine, called on process transitions

    () - starting state, [] - terminal state

                             [FAILED]
                                ^
                                |
  (WAITING) ----> FORKED ----> RUNNING -----> [KILLED]
                    |          |    |
                    v          |    `---> [SUCCESS]
                 [LOST] <------'
  """
  def on_waiting(self, process_update):
    pass

  def on_forked(self, process_update):
    pass

  def on_running(self, process_update):
    pass

  def on_success(self, process_update):
    pass

  def on_failed(self, process_update):
    pass

  def on_lost(self, process_update):
    pass

  def on_killed(self, process_update):
    pass


class TaskStateHandler(object):
  """
    Interface for handlers for the Task state machine, called on task transitions

    () - starting state, [] - terminal state

       .--------------------------------------------+----.
       |                                            |    |
       |                   .----------> [SUCCESS]   |    |
       |                   |                        |    |
       |                   | .--------> [FAILED]    |    |
       |                   | |                      |    |
    (ACTIVE)           FINALIZING ---> [KILLED] <---'    |
       |                 ^    |    .------^              |
       |                 |    |    |                     |
       `---> CLEANING ---'    `----)--> [LOST] <---------'
                | |                |      ^
                | `----------------'      |
                `-------------------------'

    ACTIVE -> KILLED/LOST only happens under garbage collection situations.
    Ordinary task preemption/kill still goes through CLEANING/FINALIZING before
    reaching a terminal state.
  """

  def on_active(self, task_update):
    pass

  def on_cleaning(self, task_update):
    pass

  def on_finalizing(self, task_update):
    pass

  def on_success(self, task_update):
    pass

  def on_failed(self, task_update):
    pass

  def on_killed(self, task_update):
    pass

  def on_lost(self, task_update):
    pass


def assert_nonempty(state, fields):
  for field in fields:
    assert getattr(state, field, None) is not None, "Missing field %s from %s!" % (field, state)


def copy_fields(state, state_update, fields):
  assert_nonempty(state_update, fields)
  for field in fields:
    setattr(state, field, getattr(state_update, field))


class CheckpointDispatcher(object):
  """
    The reconstruction/dispatching mechanism for logic triggered on task/process state transitions.

    Most applications should build an event-loop around the CheckpointDispatcher.
  """

  class Error(Exception): pass
  class ErrorRecoveringState(Error): pass
  class InvalidSequenceNumber(Error): pass
  class InvalidHandler(Error): pass

  @classmethod
  def iter_updates(cls, filename):
    try:
      with open(filename) as fp:
        rr = ThriftRecordReader(fp, RunnerCkpt)
        for update in rr:
          yield update
    except (IOError, OSError, RecordIO.Error) as err:
      raise cls.ErrorRecoveringState(err)

  @classmethod
  def iter_statuses(cls, filename):
    for update in cls.iter_updates(filename):
      if update.task_status:
        yield update.task_status

  @classmethod
  def from_file(cls, filename, truncate=False):
    """Reconstruct a RunnerState from a checkpoint stream contained in a file

      Returns a hydrated RunnerState, or None on any failures.
    """
    state = RunnerState(processes={})
    builder = cls()
    try:
      for update in cls.iter_updates(filename):
        builder.dispatch(state, update, truncate=truncate)
      return state
    except cls.Error as e:
      log.error('Failed to recover from %s: %s' % (filename, e))

  def __init__(self):
    self._task_handlers = []
    self._process_handlers = []
    self._universal_handlers = []

  def register_handler(self, handler):
    HANDLER_MAP = {
      TaskStateHandler: self._task_handlers,
      ProcessStateHandler: self._process_handlers,
      UniversalStateHandler: self._universal_handlers
    }

    for handler_type, handler_list in HANDLER_MAP.items():
      if isinstance(handler, handler_type):
        handler_list.append(handler)
        break
    else:
      raise self.InvalidHandler("Unknown handler type %s" % type(handler))

  def _run_process_dispatch(self, state, process_update):
    for handler in self._universal_handlers:
      handler.on_process_transition(state, process_update)
    for handler in self._process_handlers:
      handler_function = 'on_' + ProcessState._VALUES_TO_NAMES[state].lower()
      getattr(handler, handler_function)(process_update)

  def _run_task_dispatch(self, state, task_update):
    for handler in self._universal_handlers:
      handler.on_task_transition(state, task_update)
    for handler in self._task_handlers:
      handler_function = 'on_' + TaskState._VALUES_TO_NAMES[state].lower()
      getattr(handler, handler_function)(task_update)

  def _run_header_dispatch(self, header):
    for handler in self._universal_handlers:
      handler.on_initialization(header)

  @staticmethod
  def is_terminal(process_state_update):
    TERMINAL_STATES = [
      ProcessState.SUCCESS,
      ProcessState.FAILED,
      ProcessState.KILLED,
      ProcessState.LOST]
    return process_state_update.state in TERMINAL_STATES

  @classmethod
  def _update_process_state(cls, process_state, process_state_update):
    """
      Apply process_state_update against process_state.
      Raises ErrorRecoveringState on failure.
    """
    def assert_process_state_in(*expected_states):
      if process_state.state not in expected_states:
        raise cls.ErrorRecoveringState(
            'Detected invalid state transition %s => %s' % (
              ProcessState._VALUES_TO_NAMES.get(process_state.state),
              ProcessState._VALUES_TO_NAMES.get(process_state_update.state)))

    # CREATION => WAITING
    if process_state_update.state == ProcessState.WAITING:
      assert_process_state_in(None)
      required_fields = ['seq', 'state', 'process']
      copy_fields(process_state, process_state_update, required_fields)

    # WAITING => FORKED
    elif process_state_update.state == ProcessState.FORKED:
      assert_process_state_in(ProcessState.WAITING)
      required_fields = ['seq', 'state', 'fork_time', 'coordinator_pid']
      copy_fields(process_state, process_state_update, required_fields)

    # FORKED => RUNNING
    elif process_state_update.state == ProcessState.RUNNING:
      assert_process_state_in(ProcessState.FORKED)
      required_fields = ['seq', 'state', 'start_time', 'pid']
      copy_fields(process_state, process_state_update, required_fields)

    # RUNNING => SUCCESS
    elif process_state_update.state == ProcessState.SUCCESS:
      assert_process_state_in(ProcessState.RUNNING)
      required_fields = ['seq', 'state', 'stop_time', 'return_code']
      copy_fields(process_state, process_state_update, required_fields)

    # RUNNING => FAILED
    elif process_state_update.state == ProcessState.FAILED:
      assert_process_state_in(ProcessState.RUNNING)
      required_fields = ['seq', 'state', 'stop_time', 'return_code']
      copy_fields(process_state, process_state_update, required_fields)

    # {FORKED, RUNNING} => KILLED
    elif process_state_update.state == ProcessState.KILLED:
      assert_process_state_in(ProcessState.FORKED, ProcessState.RUNNING)
      required_fields = ['seq', 'state', 'stop_time', 'return_code']
      copy_fields(process_state, process_state_update, required_fields)

    # {FORKED, RUNNING} => LOST
    elif process_state_update.state == ProcessState.LOST:
      assert_process_state_in(ProcessState.FORKED, ProcessState.RUNNING)
      required_fields = ['seq', 'state']
      copy_fields(process_state, process_state_update, required_fields)

    else:
      raise cls.ErrorRecoveringState(
        "Unknown state = %s" % process_state_update.state)

  def would_update(self, state, runner_ckpt):
    """
      Provided a ProcessStatus, would this perform a transition and update state?
    """
    process_update = runner_ckpt.process_status
    if process_update is None:
      return False
    process = process_update.process
    if process not in state.processes: # never seen before
      return True
    else:
      # if this sequence number is ahead of the current high water mark, it would
      # produce a transition
      return state.processes[process][-1].seq < process_update.seq

  def dispatch(self, state, runner_ckpt, recovery=False, truncate=False):
    """
      Given a RunnerState and a RunnerCkpt to apply to it, determine the appropriate action and
      dispatch to the appropriate handlers.

      state          = RunnerState to be updated
      runner_ckpt    = RunnerCkpt update to apply
      recovery       = if true, enable recovery mode (accept out-of-order sequence updates)
      truncate       = if true, store only the latest task/process states, instead of
                       history for all runs.

      Raises ErrorRecoveringState on failure.
    """
    # case 1: runner_header
    #   -> Initialization of the task stream.
    if runner_ckpt.runner_header is not None:
      if state.header is not None:
        raise self.ErrorRecoveringState(
          "Attempting to rebind task with different parameters!")
      else:
        log.debug('Initializing TaskRunner header to %s' % runner_ckpt.runner_header)
        state.header = runner_ckpt.runner_header
        self._run_header_dispatch(runner_ckpt.runner_header)
      return

    # case 2: task_status
    #   -> State transition on the task (ACTIVE, FAILED, SUCCESS, LOST)
    if runner_ckpt.task_status is not None:
      if state.statuses is None:
        state.statuses = []
        old_state = None
      else:
        old_state = state.statuses[-1].state
      if not truncate:
        state.statuses.append(runner_ckpt.task_status)
      else:
        state.statuses = [runner_ckpt.task_status]
      new_state = runner_ckpt.task_status.state
      log.debug('Flipping task state from %s to %s' % (
        TaskState._VALUES_TO_NAMES.get(old_state, '(undefined)'),
        TaskState._VALUES_TO_NAMES.get(new_state, '(undefined)')))
      self._run_task_dispatch(new_state, runner_ckpt.task_status)
      return

    # case 3: process_status
    #   -> State transition on a process itself
    #        (WAITING, FORKED, RUNNING, SUCCESS, KILLED, FAILED, LOST)
    if runner_ckpt.process_status is not None:
      process_update = runner_ckpt.process_status
      name = process_update.process
      current_run = state.processes[name][-1] if name in state.processes else None
      if current_run and process_update.seq != current_run.seq + 1:
        if recovery:
          log.debug('Skipping replayed out-of-order update: %s' % process_update)
          return
        else:
          raise self.InvalidSequenceNumber(
            "Out of order sequence number! %s => %s" % (current_run, process_update))

      # One special case for WAITING: Initialize a new target ProcessState.
      if process_update.state == ProcessState.WAITING:
        assert current_run is None or self.is_terminal(current_run)
        if name not in state.processes:
          state.processes[name] = [ProcessStatus(seq=-1)]
        else:
          if not truncate:
            state.processes[name].append(ProcessStatus(seq=current_run.seq))
          else:
            state.processes[name] = [ProcessStatus(seq=current_run.seq)]

      # Run the process state machine.
      log.debug('Running state machine for process=%s/seq=%s' % (name, process_update.seq))
      if not state.processes or name not in state.processes:
        raise self.ErrorRecoveringState("Encountered potentially out of order "
          "process update.  Are you sure this is a full checkpoint stream?")
      self._update_process_state(state.processes[name][-1], process_update)
      self._run_process_dispatch(process_update.state, process_update)
      return

    raise self.ErrorRecoveringState("Empty RunnerCkpt encountered!")
