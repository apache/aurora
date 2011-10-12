import os
import sys
import tempfile
import subprocess

from twitter.tcl.loader import ThermosJobLoader
from twitter.thermos.runner import TaskRunner
from twitter.thermos.base import TaskPath
from twitter.thermos.base import AlaCarteRunnerState

from gen.twitter.thermos.ttypes import TaskState
from gen.twitter.thermos.ttypes import TaskRunState
from gen.twitter.thermos.ttypes import TaskRunnerCkpt
from gen.twitter.thermos.ttypes import TaskRunnerState
from gen.twitter.thermos.ttypes import ProcessRunState

class TestRunnerBasic(object):
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
  tasks   = tsk.replicas(1),
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
task_id = '1337'

TaskRunner(task, sandbox, root, task_id).run()
"""

  @classmethod
  def setup_class(cls):
    with open(tempfile.mktemp(), "w") as fp:
      job_filename = fp.name
      print >> fp, TestRunnerBasic.TEST_JOB_SPEC
    cls.tempdir = tempfile.mkdtemp()
    sandbox = os.path.join(cls.tempdir, 'sandbox')
    with open(tempfile.mktemp(), "w") as fp:
      script_filename = fp.name
      print >> fp, TestRunnerBasic.RUN_JOB_SCRIPT % {
        'filename': job_filename,
        'sandbox': sandbox,
        'root': cls.tempdir
      }
    cls.pathspec = TaskPath(root = cls.tempdir, task_id = '1337')
    assert subprocess.call([sys.executable, script_filename]) == 0
    cls.state = AlaCarteRunnerState(cls.pathspec.getpath('runner_checkpoint')).state()

  @classmethod
  def teardown_class(cls):
    print >> sys.stderr, 'Do cleanup here: %s' % cls.tempdir

  def test_runner_state_success(self):
    assert self.state.state == TaskState.SUCCESS

  def test_runner_header_populated(self):
    header = self.state.header
    assert header.task_id == '1337', 'header task id must be set!'
    assert header.sandbox == os.path.join(self.tempdir, 'sandbox', header.task_id), \
      'header sandbox must be set!'
    assert header.hostname, 'header task replica id must be set!'
    assert header.launch_time, 'header launch time must be set'

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

