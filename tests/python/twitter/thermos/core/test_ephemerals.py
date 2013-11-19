from twitter.thermos.config.schema import (
  Task,
  Resources,
  Process)
from twitter.thermos.testing.runner import RunnerTestBase
from gen.twitter.thermos.ttypes import (
  TaskState,
  ProcessState
)


class TestEphemeralTask(RunnerTestBase):
  @classmethod
  def task(cls):
    task = Task(
      name = "task_with_ephemeral",
      processes = [
        Process(name = "ephemeral_sleepy", ephemeral=True, cmdline="sleep 10"),
        Process(name = "sleepy", cmdline="sleep 1")
      ])
    return task.interpolate()[0]

  def test_runner_state(self):
    assert self.state.statuses[-1].state == TaskState.SUCCESS

  def test_runner_process_in_expected_states(self):
    processes = self.state.processes
    # sleepy succeeded
    assert len(processes['sleepy']) == 1
    assert processes['sleepy'][0].state == ProcessState.SUCCESS
    # ephemeral sleepy was terminated because sleepy finished and it's ephemeral
    assert len(processes['ephemeral_sleepy']) == 1
    assert processes['ephemeral_sleepy'][0].state == ProcessState.KILLED
