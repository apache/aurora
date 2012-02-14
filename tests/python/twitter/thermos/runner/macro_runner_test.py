import os
import pytest
from runner_base import RunnerTestBase

from twitter.thermos.config.schema import Task, Process, Resources
from twitter.thermos.config.dsl import with_schedule
from gen.twitter.thermos.ttypes import (
  TaskState,
  ProcessState
)

class TestRunnerBasic(RunnerTestBase):
  portmap = {'named_port': 8123}

  @classmethod
  def task(cls):
    hello_template = Process(cmdline = "echo 1")
    t1 = hello_template(name = "t1", cmdline = "echo 1 port {{thermos.ports[named_port]}}")
    t2 = hello_template(name = "t2")
    t3 = hello_template(name = "t3")
    t4 = hello_template(name = "t4")
    t5 = hello_template(name = "t5")
    t6 = hello_template(name = "t6")
    tsk = with_schedule(Task(
      name = "complex",
      processes = [t1, t2, t3, t4, t5, t6],
      resources = Resources(cpu = 1.0, ram = 16*1024*1024, disk = 16*1024)),
      "t1 before t3", "t1 before t4", "t2 before t3", "t2 before t4",
      "t3 before t5", "t3 before t6", "t4 before t5", "t4 before t6")
    return tsk

  def test_runner_state_reconstruction(self):
    assert self.state == self.reconstructed_state

  def test_runner_state_success(self):
    assert self.state.statuses[-1].state == TaskState.SUCCESS

  def test_runner_header_populated(self):
    header = self.state.header
    assert header is not None, 'header should be populated.'
    assert header.task_id == self.task_id, 'header task id must be set!'
    assert header.sandbox == os.path.join(self.tempdir, 'sandbox', header.task_id), \
      'header sandbox must be set!'
    assert header.hostname, 'header task replica id must be set!'
    assert header.launch_time_ms, 'header launch time must be set'

  def test_runner_has_allocated_name_ports(self):
    ports = self.state.header.ports
    assert 'named_port' in ports, 'ephemeral port was either not allocated, or not checkpointed!'
    assert ports['named_port'] == 8123

  def test_runner_has_expected_processes(self):
    processes = self.state.processes
    process_names = set(['t%d'%k for k in range(1,7)])
    actual_process_names = set(processes.keys())
    assert process_names == actual_process_names, "runner didn't run expected set of processes!"
    for process in processes:
      assert processes[process][-1].process == process

  def test_runner_processes_have_expected_output(self):
    for process in self.state.processes:
      history = self.state.processes[process]
      assert history[-1].state == ProcessState.SUCCESS
      if len(history) > 1:
        for run in range(len(history)-1):
          assert history[run].state != ProcessState.SUCCESS, \
            "nonterminal processes must not be in SUCCESS state!"

  def test_runner_processes_have_monotonically_increasing_timestamps(self):
    for process in self.state.processes:
      for run in self.state.processes[process]:
        assert run.fork_time < run.start_time
        assert run.start_time < run.stop_time
