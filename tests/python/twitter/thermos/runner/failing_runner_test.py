from runner_base import RunnerTestBase

from gen.twitter.thermos.ttypes import (
  TaskState,
  ProcessRunState
)

class TestFailingRunner(RunnerTestBase):
  JOB_SPEC = """
import getpass
ping_template = Process(
  max_failures=5,
  cmdline = "echo {{process.name}} pinging;                                       "
            "echo ping >> {{process.name}};                                       "
            "if [ $(cat {{process.name}} | wc -l) -eq {{params.num_runs}} ]; then "
            "  exit 0;                                                             "
            "else                                                                 "
            "  exit 1;                                                            "
            "fi                                                                   ")

# TODO(wickman)  Fix the bug with params propagation that prevents us from
# passing params via ping_template.
p1 = ping_template(name = "p1")
p1._params={'num_runs':1}
p2 = ping_template(name = "p2")
p2._params={'num_runs':2}
p3 = ping_template(name = "p3")
p3._params={'num_runs':3}

tsk = Task(name = "pingping").with_processes(p1, p2, p3)

hello_job = Job(
  tasks   = tsk.replicas(1),
  name    = 'hello_world',
  role    = getpass.getuser(),
  dc      = 'smf1',
  cluster = 'smf1-test')

hello_job.export()
"""

  @classmethod
  def job_specification(cls):
    return TestFailingRunner.JOB_SPEC

  def test_runner_state_reconstruction(self):
    assert self.state == self.reconstructed_state

  def test_runner_state_success(self):
    assert self.state.state == TaskState.SUCCESS

  def test_runner_processes_have_expected_runs(self):
    processes = self.state.processes
    for k in range(1,4):
      process_name = 'p%d' % k
      assert process_name in processes
      assert len(processes[process_name].runs) == k
      for j in range(k-1):
        assert processes[process_name].runs[j].run_state == ProcessRunState.FAILED
      assert processes[process_name].runs[k-1].run_state == ProcessRunState.FINISHED
