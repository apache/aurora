from runner_base import RunnerTestBase

from twitter.thermos.config.schema import Task, Resources, Process
from gen.twitter.thermos.ttypes import (
  TaskState,
  ProcessState
)

class TestFailingRunner(RunnerTestBase):
  @classmethod
  def task(cls):
    ping_template = Process(
      name="{{name}}",
      min_duration=1,
      max_failures=5,
      cmdline = "echo {{name}} pinging;                                "
                "echo ping >> {{name}};                                "
                "echo current count $(cat {{name}} | wc -l);           "
                "if [ $(cat {{name}} | wc -l) -eq {{num_runs}} ]; then "
                "  exit 0;                                             "
                "else                                                  "
                "  exit 1;                                             "
                "fi                                                    ")
    tsk = Task(
      name = "pingping",
      resources = Resources(cpu = 1.0, ram = 16*1024*1024, disk = 16*1024),
      processes = [
        ping_template.bind(name = "p1", num_runs = 1),
        ping_template.bind(name = "p2", num_runs = 2),
        ping_template.bind(name = "p3", num_runs = 3),
      ]
    )
    return tsk.interpolate()[0]

  def test_runner_state_success(self):
    assert self.state.statuses[-1].state == TaskState.SUCCESS

  def test_runner_processes_have_expected_runs(self):
    processes = self.state.processes
    for k in range(1,4):
      process_name = 'p%d' % k
      assert process_name in processes
      assert len(processes[process_name]) == k
      for j in range(k-1):
        assert processes[process_name][j].state == ProcessState.FAILED
      assert processes[process_name][k-1].state == ProcessState.SUCCESS
