import copy

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader
from thermos_thrift.ttypes import *

class WorkflowCkpt_ErrorRecoveringState(Exception): pass
class WorkflowCkpt_InvalidStateTransition(Exception): pass
class WorkflowCkpt_InvalidSequenceNumber(Exception): pass

class WorkflowCkptDispatcher:
  """
    The reconstruction / dispatching mechanism for logic triggered on
    workflow/task state transitions.

    Most applications should build an event-loop around the
    WorkflowCkptDispatcher.
  """
  def __init__(self):
    self._state_handlers = {}
    self._universal_handlers = []
    self._port_handlers = []

  def register_state_handler(self, run_state, function):
    """
      Register a function callback for when a task transitions its run state.
      Current run states: WAITING, FORKED, RUNNING, FINISHED, KILLED, FAILED, LOST.
    """
    if run_state not in self._state_handlers:
      self._state_handlers[run_state] = []
    self._state_handlers[run_state].append(function)

  def register_port_handler(self, function):
    """
      Register a function callback to handle when a port is allocated by the runner.
    """
    self._port_handlers.append(function)

  def register_universal_handler(self, function):
    """
      Register a function callback that gets called on every task state transition.
    """
    self._universal_handlers.append(function)

  def _run_state_dispatch(self, state, task_update):
    for handler in self._universal_handlers:
      handler(task_update)
    for handler in self._state_handlers.get(state, []):
      handler(task_update)

  def _run_port_dispatch(self, name, port):
    for handler in self._port_handlers:
      handler(name, port)

  @staticmethod
  def check_empty_fields(task_state, fields):
    for field in fields:
      if task_state.__dict__[field] is not None:
        raise WorkflowCkpt_ErrorRecoveringState("Field field %s from %s should be empty, instead got: %s" % (
          field, task_state, task_state.__dict__[field]))

  @staticmethod
  def check_nonempty_fields(task_state, fields):
    for field in fields:
      if task_state.__dict__[field] is None:
        raise WorkflowCkpt_ErrorRecoveringState("Missing field %s from %s!" % (field, task_state))

  @staticmethod
  def check_and_copy_fields(task_state, task_state_update, fields):
    WorkflowCkptDispatcher.check_empty_fields(task_state, fields)
    WorkflowCkptDispatcher.check_nonempty_fields(task_state_update, fields)
    for field in fields:
      task_state.__dict__[field] = copy.deepcopy(task_state_update.__dict__[field])

  @staticmethod
  def copy_fields(task_state, task_state_update, fields):
    WorkflowCkptDispatcher.check_nonempty_fields(task_state_update, fields)
    for field in fields:
      task_state.__dict__[field] = copy.deepcopy(task_state_update.__dict__[field])

  @staticmethod
  def is_terminal(task_state_update):
    TERMINAL_STATES = [
      WorkflowTaskRunState.FINISHED,
      WorkflowTaskRunState.FAILED,
      WorkflowTaskRunState.LOST]
    return task_state_update.run_state in TERMINAL_STATES

  def update_workflow_state(self, task_state, task_state_update, recovery):
    """
      Apply task_state_update against task_state.

      set recovery = True if you are in checkpoint recovery mode (i.e. you expect
        to see replays of ckpts from forked children.)

      returns True if a state update was applied to task_state
    """
    if task_state_update.seq is None:
      raise WorkflowCkpt_InvalidSequenceNumber(
        "Got nil suquence number! update = %s" % task_state_update)

    # Special-casing seq == 0 is kind of blech.  Should we create an INIT state?
    if task_state.seq > 0:
      if task_state_update.seq <= task_state.seq:
        if recovery:
          # in recovery mode, we expect to see out of order updates from
          # task checkpoints since we are starting over at sequence number
          # 0.  if not in recovery mode, this is an error.
          return False
        else:
          raise WorkflowCkpt_InvalidSequenceNumber("Out of order sequence number! %s => %s" % (
            task_state, task_state_update))

      # We should not see non-contiguous sequence numbers, but keep it at a
      # warning for now until we're certain there are no bugs.
      if task_state_update.seq != task_state.seq + 1:
        log.error("WARNING: Noncontiguous sequence number: %s => %s" % (
          task_state, task_state_update))

    # see thrift/thermos_runner.thrift for more explanation of the state transitions
    if task_state_update.run_state is not None:
      # [CREATION] => WAITING
      if task_state_update.run_state == WorkflowTaskRunState.WAITING:
        required_fields = ['seq', 'run_state', 'task']
        WorkflowCkptDispatcher.copy_fields(task_state, task_state_update, required_fields)

      # WAITING => FORKED
      elif task_state_update.run_state == WorkflowTaskRunState.FORKED:
        if task_state.run_state != WorkflowTaskRunState.WAITING:
          raise WorkflowCkpt_InvalidStateTransition("%s => %s" % (task_state, task_state_update))
        required_fields = ['seq', 'run_state', 'fork_time', 'runner_pid']
        WorkflowCkptDispatcher.copy_fields(task_state, task_state_update, required_fields)

      # FORKED => RUNNING
      elif task_state_update.run_state == WorkflowTaskRunState.RUNNING:
        if task_state.run_state != WorkflowTaskRunState.FORKED:
          raise WorkflowCkpt_InvalidStateTransition("%s => %s" % (task_state, task_state_update))
        required_fields = ['seq', 'run_state', 'start_time', 'pid']
        WorkflowCkptDispatcher.copy_fields(task_state, task_state_update, required_fields)

      # RUNNING => FINISHED
      elif task_state_update.run_state == WorkflowTaskRunState.FINISHED:
        if task_state.run_state != WorkflowTaskRunState.RUNNING:
          raise WorkflowCkpt_InvalidStateTransition("%s => %s" % (task_state, task_state_update))
        required_fields = ['seq', 'run_state', 'stop_time', 'return_code']
        WorkflowCkptDispatcher.copy_fields(task_state, task_state_update, required_fields)

      # RUNNING => FAILED
      elif task_state_update.run_state == WorkflowTaskRunState.FAILED:
        if task_state.run_state != WorkflowTaskRunState.RUNNING:
          raise WorkflowCkpt_InvalidStateTransition("%s => %s" % (task_state, task_state_update))
        required_fields = ['seq', 'run_state', 'stop_time', 'return_code']
        WorkflowCkptDispatcher.copy_fields(task_state, task_state_update, required_fields)

      # {FORKED, RUNNING} => LOST
      elif task_state_update.run_state == WorkflowTaskRunState.LOST:
        if task_state.run_state not in (WorkflowTaskRunState.FORKED, WorkflowTaskRunState.RUNNING):
          raise WorkflowCkpt_InvalidStateTransition("%s => %s" % (task_state, task_state_update))
        required_fields = ['seq', 'run_state']
        WorkflowCkptDispatcher.copy_fields(task_state, task_state_update, required_fields)

      else:
        raise WorkflowCkpt_ErrorRecoveringState("Unknown run_state = %s" % task_state_update.run_state)

    # dispatch state change to consumer
    self._run_state_dispatch(task_state_update.run_state, task_state_update)
    return True

  @staticmethod
  def _check_runner_ckpt_sanity(runner_ckpt):
    # Check sanity
    changes = 0
    if runner_ckpt.runner_header is not None: changes += 1
    if runner_ckpt.task_state is not None: changes += 1
    if runner_ckpt.allocated_port is not None: changes += 1
    if runner_ckpt.history_state_update is not None: changes += 1
    if runner_ckpt.state_update is not None: changes += 1
    if changes > 1:
      raise WorkflowCkpt_InvalidStateTransition(
        "Multiple checkpoint types in the same message! %s" % runner_ckpt)

  # Provided a WorkflowTaskState, would this perform a transition and update state?
  def would_update(self, state, runner_ckpt):
    task_update = runner_ckpt.task_state
    if task_update is None:
      return False

    task = task_update.task
    if task not in state.tasks:   # never seen before
      return True
    else:
      # We have seen this task, so the state update must pertain to the current run.
      wth = state.tasks[task]
      wts = wth.runs[-1]
      # if this sequence number is ahead of the current high water mark, it would
      # produce a transition
      return wts.seq < task_update.seq

  # raises exception if something goes wrong
  # returns True if checkpoint record was applied, false otherwise
  def update_runner_state(self, state, runner_ckpt, recovery = False):
    """
      state       = WorkflowRunnerState to apply task update
      task_update = WorkflowRunnerCkpt update
      recovery    => Pass in as true if you are in recovery mode

      returns True if task_update was applied to state.
    """
    wts, wth = None, None

    self._check_runner_ckpt_sanity(runner_ckpt)

    # case 1: runner_header
    #   -> Initialization of the workflow stream.
    if runner_ckpt.runner_header is not None:
      if state.header is not None:
        raise WorkflowCkpt_ErrorRecoveringState(
          "Multiple headers encountered in WorkflowRunnerState!")
      else:
        state.header = runner_ckpt.runner_header
        return True

    # case 2: allocated_port
    #   -> Allocated a named ephemeral port to a task
    if runner_ckpt.allocated_port is not None:
      port, port_name = runner_ckpt.allocated_port.port, runner_ckpt.allocated_port.port_name
      if state.ports is None:
        state.ports = {}
      if port_name in state.ports:
        if port != state.ports[port_name]:
          raise WorkflowCkpt_ErrorRecoveringState(
            "Port assignment conflicts with earlier assignment: %s" % port_name)
        else:
          return False
      else:
        state.ports[port_name] = port
        self._run_port_dispatch(port_name, port)
        return True

    # case 3: state_update
    #   -> State transition on the workflow (ACTIVE, FAILED, FINISHED)
    if runner_ckpt.state_update is not None:
      if state.state != runner_ckpt.state_update.state:
        state.state = runner_ckpt.state_update.state
        return True
      return False

    # case 4: history_state_update
    #   -> State transition on the run of a task within the workflow (ACTIVE, FAILED, FINISHED)
    if runner_ckpt.history_state_update is not None:
      task_name, state_change = runner_ckpt.history_state_update.task, runner_ckpt.history_state_update.state
      if state.tasks[task_name].state != state_change:
        state.tasks[task_name].state = state_change
        return True
      return False

    # case 5: task_state
    #   -> State transition on a task itself (WAITING, FORKED, RUNNING, FINISHED, KILLED, FAILED, LOST)
    task_update = runner_ckpt.task_state
    if task_update is None:
      log.error(WorkflowCkpt_ErrorRecoveringState("Empty WorkflowRunnerCkpt encountered!"))
      return False

    task = task_update.task
    if task not in state.tasks:
      # Never seen this task before, create a TaskHistory for it and initialize run 0.
      wts = WorkflowTaskState(seq = 0, task = task, run_state = WorkflowTaskRunState.WAITING)
      wth = WorkflowTaskHistory(task = task, runs = [], state = WorkflowState.ACTIVE)
      wth.runs.append(wts)
      state.tasks[task] = wth

    else:
      # We have seen this task, so the state update must pertain to the current run.
      wth = state.tasks[task]
      wts = wth.runs[-1]

      # Cannot have two consecutive terminal states
      #
      # You can go nonterminal=>nonterminal (normal), nonterminal=>terminal (success), and
      # terminal=>nonterminal (lost/failure) but not terminal=>terminal, so sanity check that.
      if WorkflowCkptDispatcher.is_terminal(wth.runs[-1]):
        if WorkflowCkptDispatcher.is_terminal(task_update):
          raise WorkflowCkpt_ErrorRecoveringState(
            "Received two consecutive terminal task states: wts=%s update=%s" % (
            wts, task_update))
        else:
          # We transitioned from terminal => nonterminal, so finish up the current run and
          # initialize a new "latest run" for the task.
          wts = WorkflowTaskState(seq = wts.seq, task = task, run_state = WorkflowTaskRunState.WAITING)
          wth.runs.append(wts)

    # Run the task state machine.
    return self.update_workflow_state(wts, task_update, recovery)

class AlaCarteRunnerState:
  """
    Helper class to reconstruct a WorkflowRunnerCkpt from its checkpoint file without any fuss.
  """

  def __init__(self, path):
    self._state = WorkflowRunnerState(tasks = {})
    builder = WorkflowCkptDispatcher()

    try:
      fp = open(path, "r")
      rr = ThriftRecordReader(fp, WorkflowRunnerCkpt)
      for wrc in rr:
        builder.update_runner_state(self._state, wrc)
    except Exception, e:
      log.error('Error recovering AlaCarteRunnerState(%s): %s' % (path, e))
      self._state = None
    finally:
      fp.close()

  def state(self):
    return self._state
