from runner_base import RunnerTestBase
from twitter.thermos.config.schema import (
  Task,
  Resources,
  Process,
  ProcessConstraint,
  ProcessPair)
from gen.twitter.thermos.ttypes import (
  TaskState,
  ProcessState
)

class TestFailureLimit(RunnerTestBase):
  @classmethod
  def task(cls):
    task = Task(
      name = "failing_task",
      resources = Resources(cpu = 1.0, ram = 16*1024*1024, disk = 16*1024),
      max_failures = 2,
      processes = [
        Process(name = "a", max_failures=1, cmdline="echo hello world"),
        Process(name = "b", max_failures=1, cmdline="exit 1"),
        Process(name = "c", max_failures=1, cmdline="echo hello world")
      ],
      constraints = [
        ProcessConstraint(ordered = [ProcessPair(first = 'a', second = 'b')]),
        ProcessConstraint(ordered = [ProcessPair(first = 'b', second = 'c')]),
      ]
    )
    return task.interpolate()[0]

  def test_runner_state_reconstruction(self):
    assert self.state == self.reconstructed_state

  def test_runner_state_failure(self):
    assert self.state.statuses[-1].state == TaskState.FAILED

  def test_runner_process_in_expected_states(self):
    processes = self.state.processes
    assert len(processes['a']) == 1
    assert processes['a'][0].state == ProcessState.SUCCESS
    assert len(processes['b']) == 1
    assert processes['b'][0].state == ProcessState.FAILED
    assert 'c' not in processes
