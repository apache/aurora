import os
import signal
import sys
import threading
import time

from runner_base import Runner

from twitter.common.quantity import Amount, Time
from twitter.thermos.config.schema import (
  Task,
  Resources,
  Process)
from twitter.thermos.monitoring.monitor import TaskMonitor
from twitter.thermos.runner.runner import TaskRunner
from gen.twitter.thermos.ttypes import (
  TaskState,
  ProcessState
)


sleepy_process = Process(
  name = "sleepy",
  cmdline = "sleep 3",
  min_duration = 1)

ignore_script = [
  "import time, signal",
  "signal.signal(signal.SIGTERM, signal.SIG_IGN)",
  "time.sleep(1000)"
]

ignorant_process = Process(
  name="ignorant",
  cmdline="%s -c '%s'" % (sys.executable, ';'.join(ignore_script)),
  min_duration=1)


class RunnerBase(object):
  @classmethod
  def task(cls):
    raise NotImplementedError

  @classmethod
  def start_runner(cls):
    runner = Runner(cls.task())
    class RunThread(threading.Thread):
      def run(self):
        runner.run()
    rt = RunThread()
    rt.start()
    return runner

  @classmethod
  def wait_until_running(cls, monitor):
    while True:
      active_processes = monitor.get_active_processes()
      if len(active_processes) == 0:
        time.sleep(0.1)
      else:
        assert len(active_processes) == 1
        if active_processes[0][0].pid is None:
          time.sleep(0.1)
        else:
          break


class ProcessPidTestCase(object):
  def test_process_kill(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.pathspec, runner.task_id)
    self.wait_until_running(tm)

    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'process'
    assert run_number == 0
    os.kill(process_state.pid, signal.SIGKILL)

    while True:
      if not hasattr(runner, 'state'):
        time.sleep(0.1)
      else:
        break

    assert runner.state.statuses[-1].state == TaskState.SUCCESS
    assert 'process' in runner.state.processes
    assert len(runner.state.processes['process']) == 2
    assert runner.state.processes['process'][0].state == ProcessState.KILLED
    assert runner.state.processes['process'][0].return_code == -signal.SIGKILL
    assert runner.state.processes['process'][1].state == ProcessState.SUCCESS


class TestRunnerKill(RunnerBase, ProcessPidTestCase):
  @classmethod
  def task(cls):
    task = Task(name = "task", processes = [sleepy_process(name="process")])
    return task.interpolate()[0]

  def test_coordinator_kill(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.pathspec, runner.task_id)
    self.wait_until_running(tm)

    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'process'
    assert run_number == 0
    os.kill(process_state.coordinator_pid, signal.SIGKILL)

    while True:
      if not hasattr(runner, 'state'):
        time.sleep(0.1)
      else:
        break

    assert runner.state.statuses[-1].state == TaskState.SUCCESS
    assert 'process' in runner.state.processes
    assert len(runner.state.processes['process']) == 2
    assert runner.state.processes['process'][0].state == ProcessState.LOST
    assert runner.state.processes['process'][1].state == ProcessState.SUCCESS


class TestRunnerKillProcessTrappingSIGTERM(RunnerBase):
  @classmethod
  def task(cls):
    task = Task(name = "task", processes = [ignorant_process(name="process")])
    return task.interpolate()[0]

  def test_coordinator_kill(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.pathspec, runner.task_id)
    self.wait_until_running(tm)
    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'process'
    assert run_number == 0
    os.kill(process_state.coordinator_pid, signal.SIGKILL)

    while True:
      active_procs = tm.get_active_processes()
      if active_procs and active_procs[0][1] > 0:
        break
      time.sleep(0.2)
    self.wait_until_running(tm)

    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'process'
    assert run_number == 1
    os.kill(process_state.pid, signal.SIGKILL)

    while True:
      active_procs = tm.get_active_processes()
      if active_procs and active_procs[0][1] > 1:
        break
      time.sleep(0.2)
    self.wait_until_running(tm)

    os.kill(runner.po.pid, signal.SIGKILL)

    try:
      state = tm.get_state()
      assert state.processes['process'][0].state == ProcessState.LOST
      assert state.processes['process'][1].state == ProcessState.KILLED
      assert state.processes['process'][2].state == ProcessState.RUNNING
    finally:
      os.kill(state.processes['process'][2].coordinator_pid, signal.SIGKILL)
      os.kill(state.processes['process'][2].pid, signal.SIGKILL)

  def test_preemption_wait(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.pathspec, runner.task_id)
    self.wait_until_running(tm)
    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'process'
    assert run_number == 0

    preempter = TaskRunner.get(runner.task_id, runner.root)
    assert preempter is not None
    now = time.time()
    preempter.kill(force=True, preemption_wait=Amount(1, Time.SECONDS))
    duration = time.time() - now

    # This is arbitrary, but make sure we finish within half a second of
    # requested preemption wait.
    assert abs(duration - 1.0) < 0.5

    assert preempter.state.statuses[-1].state == TaskState.KILLED
    assert preempter.state.processes['process'][-1].state == ProcessState.KILLED
