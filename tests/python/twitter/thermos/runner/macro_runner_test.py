import os
import pytest
from runner_base import RunnerTestBase

from twitter.thermos.config.schema import Task, Process, Resources
from twitter.thermos.config.dsl import with_schedule
from gen.twitter.thermos.ttypes import (
  TaskState,
  TaskRunState,
  ProcessRunState
)

class TestRunnerBasic(RunnerTestBase):
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
    ports = self.state.ports
    assert 'named_port' in ports, 'ephemeral port was either not allocated, or not checkpointed!'

  def test_runner_has_expected_processes(self):
    processes = self.state.processes

    process_names = set(['t%d'%k for k in range(1,7)])
    actual_process_names = set(processes.keys())
    assert process_names == actual_process_names, "runner didn't run expected set of processes!"
    for process in processes:
      assert processes[process].process == process

  def test_runner_processes_have_expected_output(self):
    processes = self.state.processes
    for process in processes:
      history = processes[process]
      assert history.state == TaskRunState.SUCCESS, (
        "runner didn't succeed!  instead state = %s" % (history.state))
      if len(history.runs) > 1:
        for run in range(len(history.runs)-1):
          assert history.runs[run].run_state != ProcessRunState.FINISHED, \
            "nonterminal processes must not be in FINISHED state!"
      last_run = history.runs[-1]
      assert last_run.run_state == ProcessRunState.FINISHED, \
        "terminal processes must be in FINISHED state!"
      assert last_run.process == process, "process (%s) had unexpected checkpointed name: %s!" % (
        process, last_run.process)

  def test_runner_processes_have_monotonically_increasing_timestamps(self):
    processes = self.state.processes
    for process in processes:
      history = processes[process]
      for run in history.runs:
        assert run.fork_time < run.start_time
        assert run.start_time < run.stop_time

