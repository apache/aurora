import os
import sys
import tempfile
import subprocess

import unittest
import pytest

from twitter.tcl.loader import ThermosJobLoader
from twitter.thermos.runner import TaskRunner
from twitter.thermos.base import TaskPath
from twitter.thermos.base import AlaCarteRunnerState

from thermos_thrift.ttypes import TaskState
from thermos_thrift.ttypes import TaskRunState
from thermos_thrift.ttypes import TaskRunnerCkpt
from thermos_thrift.ttypes import TaskRunnerState
from thermos_thrift.ttypes import ProcessRunState

class RunnerMacroTest(unittest.TestCase):
  TEST_JOB_SPEC = """
import getpass
hello_template = Process(cmdline = "echo {{task.replica_id}}")

# create special process with named port
t1 = hello_template(name = "t1")(cmdline = "echo {{task.replica_id}} port %port:named_port%")
t2 = hello_template(name = "t2")
t3 = hello_template(name = "t3")
t4 = hello_template(name = "t4")
t5 = hello_template(name = "t5")
t6 = hello_template(name = "t6")

tsk = Task(name = "complex")
tsk = tsk.with_processes(t1, t2)
tsk = tsk.with_processes([t3, t4])
tsk = tsk.with_processes(t5, t6)

hello_job = Job(
  processes   = tsk.replicas(1),
  name    = 'hello_world',
  role    = getpass.getuser(),
  dc      = 'smf1',
  cluster = 'smf-test')

hello_job.export()
"""

  # unittest/pytest, Y U NO WORK WITH os.fork?!?!?!
  RUN_JOB_SCRIPT = """
from twitter.tcl.loader import ThermosJobLoader
from twitter.thermos.runner import TaskRunner

job = ThermosJobLoader('%(filename)s').to_thrift()
task = job.tasks[0]
sandbox = '%(sandbox)s'
root = '%(root)s'
job_uid = 1337

TaskRunner(task, sandbox, root, job_uid).run()
"""

  @classmethod
  def setup_class(cls):
    with open(tempfile.mktemp(), "w") as fp:
      job_filename = fp.name
      print >> fp, RunnerMacroTest.TEST_JOB_SPEC
    cls.tempdir = tempfile.mkdtemp()
    sandbox = os.path.join(cls.tempdir, 'sandbox')
    with open(tempfile.mktemp(), "w") as fp:
      script_filename = fp.name
      print >> fp, RunnerMacroTest.RUN_JOB_SCRIPT % {
        'filename': job_filename,
        'sandbox': sandbox,
        'root': cls.tempdir
      }
    cls.pathspec = TaskPath(root = cls.tempdir, job_uid = 1337)
    assert subprocess.call([sys.executable, script_filename]) == 0
    cls.state = AlaCarteRunnerState(cls.pathspec.getpath('runner_checkpoint')).state()

  @classmethod
  def teardown_class(cls):
    print >> sys.stderr, 'Do cleanup here: %s' % cls.tempdir

  def test_runner_state_success(self):
    assert self.state.state == TaskState.SUCCESS

  def test_runner_header_populated(self):
    header = self.state.header
    assert header.job_name == 'hello_world', 'header job name must be set!'
    assert header.job_uid == 1337, 'header job uid must be set!'
    assert header.task_name == 'complex', 'header task name must be set!'
    assert header.task_replica == 0, 'header task replica id must be set!'
    # can do any reasonable assertions against .launch_time or hostname?

  def test_runner_has_allocated_name_ports(self):
    ports = self.state.ports
    assert 'named_port' in ports, 'ephemeral port was either not allocated, or not checkpointed!'

  def test_runner_has_expected_processes(self):
    processes = self.state.processes

    process_names = set(['t%d'%k for k in range(1,7)])
    actual_process_names = set(processes.keys())
    assert process_names == actual_process_names, "runner didn't run expected set of processes!"

    # egh
    for process in processes:
      assert processes[process].process == process

  def test_runner_processes_have_expected_output(self):
    processes = self.state.processes
    for process in processes:
      history = processes[process]
      assert history.state == TaskRunState.SUCCESS, "runner didn't succeed!"
      if len(history.runs) > 1:
        for run in range(len(history.runs)-1):
          # WorkflowTaskRunState=>ProcessRunState
          assert history.runs[run].run_state != ProcessRunState.FINISHED, \
            "nonterminal processes must not be in FINISHED state!"
      last_run = history.runs[-1]
      # WorkflowTaskRunState=>ProcessRunState
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

