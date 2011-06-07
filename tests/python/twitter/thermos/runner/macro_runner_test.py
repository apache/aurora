import os
import sys
import tempfile
import subprocess

import unittest
import pytest

from twitter.tcl.loader import ThermosJobLoader
from twitter.thermos.runner import WorkflowRunner
from twitter.thermos.base import WorkflowPath
from twitter.thermos.base import AlaCarteRunnerState

from thermos_thrift.ttypes import WorkflowState
from thermos_thrift.ttypes import WorkflowRunState
from thermos_thrift.ttypes import WorkflowRunnerCkpt
from thermos_thrift.ttypes import WorkflowRunnerState
from thermos_thrift.ttypes import WorkflowTaskRunState

class RunnerMacroTest(unittest.TestCase):
  TEST_JOB_SPEC = """
import getpass
hello_template = Task(cmdline = "echo {{workflow.replica_id}}")

# create special task with named port
t1 = hello_template(name = "t1")(cmdline = "echo {{workflow.replica_id}} port %port:named_port%")
t2 = hello_template(name = "t2")
t3 = hello_template(name = "t3")
t4 = hello_template(name = "t4")
t5 = hello_template(name = "t5")
t6 = hello_template(name = "t6")

wf = Workflow(name = "complex")
wf = wf.with_tasks(t1, t2)
wf = wf.with_tasks([t3, t4])
wf = wf.with_tasks(t5, t6)

hello_job = Job(
  tasks   = wf.replicas(1),
  name    = 'hello_world',
  role    = getpass.getuser(),
  dc      = 'smf1',
  cluster = 'smf-test')

hello_job.export()
"""

  # unittest/pytest, Y U NO WORK WITH os.fork?!?!?!
  RUN_JOB_SCRIPT = """
from twitter.tcl.loader import ThermosJobLoader
from twitter.thermos.runner import WorkflowRunner

job = ThermosJobLoader('%(filename)s').to_thrift()
workflow = job.workflows[0]
sandbox = '%(sandbox)s'
root = '%(root)s'
job_uid = 1337

WorkflowRunner(workflow, sandbox, root, job_uid).run()
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
    cls.pathspec = WorkflowPath(root = cls.tempdir, job_uid = 1337)
    assert subprocess.call([sys.executable, script_filename]) == 0
    cls.state = AlaCarteRunnerState(cls.pathspec.getpath('runner_checkpoint')).state()

  @classmethod
  def teardown_class(cls):
    print >> sys.stderr, 'Do cleanup here: %s' % cls.tempdir

  def test_runner_state_success(self):
    assert self.state.state == WorkflowState.SUCCESS

  def test_runner_header_populated(self):
    header = self.state.header
    assert header.job_name == 'hello_world', 'header job name must be set!'
    assert header.job_uid == 1337, 'header job uid must be set!'
    assert header.workflow_name == 'complex', 'header workflow name must be set!'
    assert header.workflow_replica == 0, 'header workflow replica id must be set!'
    # can do any reasonable assertions against .launch_time or hostname?

  def test_runner_has_allocated_name_ports(self):
    ports = self.state.ports
    assert 'named_port' in ports, 'ephemeral port was either not allocated, or not checkpointed!'

  def test_runner_has_expected_tasks(self):
    tasks = self.state.tasks

    task_names = set(['t%d'%k for k in range(1,7)])
    actual_task_names = set(tasks.keys())
    assert task_names == actual_task_names, "runner didn't run expected set of tasks!"

    # egh
    for task in tasks:
      assert tasks[task].task == task

  def test_runner_tasks_have_expected_output(self):
    tasks = self.state.tasks
    for task in tasks:
      history = tasks[task]
      assert history.state == WorkflowRunState.SUCCESS, "runner didn't succeed!"
      if len(history.runs) > 1:
        for run in range(len(history.runs)-1):
          assert history.runs[run].run_state != WorkflowTaskRunState.FINISHED, \
            "nonterminal tasks must not be in FINISHED state!"
      last_run = history.runs[-1]
      assert last_run.run_state == WorkflowTaskRunState.FINISHED, \
        "terminal tasks must be in FINISHED state!"
      assert last_run.task == task, "task (%s) had unexpected checkpointed name: %s!" % (
        task, last_run.task)

  def test_runner_tasks_have_monotonically_increasing_timestamps(self):
    tasks = self.state.tasks
    for task in tasks:
      history = tasks[task]
      for run in history.runs:
        assert run.fork_time < run.start_time
        assert run.start_time < run.stop_time

