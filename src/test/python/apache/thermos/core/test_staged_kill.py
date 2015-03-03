#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import signal
import sys
import threading
import time

import pytest
from twitter.common.process import ProcessProviderFactory
from twitter.common.quantity import Amount, Time

from apache.thermos.config.schema import Process, Task
from apache.thermos.core.runner import TaskRunner
from apache.thermos.monitoring.monitor import TaskMonitor
from apache.thermos.testing.runner import Runner

from gen.apache.thermos.ttypes import ProcessState, TaskState

sleepy_process = Process(name="sleepy", cmdline="sleep 3", min_duration=1)

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
      procs = monitor.get_state().processes
      if 'process' in procs:
        # check the process hasn't died unexpectedly
        assert procs['process'][0].return_code is None
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
    tm = TaskMonitor(runner.tempdir, runner.task_id)
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
    task = Task(name="task", processes=[sleepy_process(name="process")])
    return task.interpolate()[0]

  def test_coordinator_kill(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.tempdir, runner.task_id)
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
    task = Task(name="task",
                finalization_wait=3,
                processes=[ignorant_process(name="ignorant_process")])
    return task.interpolate()[0]

  def test_coordinator_kill(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.tempdir, runner.task_id)
    self.wait_until_running(tm)
    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'ignorant_process'
    assert run_number == 0
    os.kill(process_state.coordinator_pid, signal.SIGKILL)

    while True:
      active_procs = tm.get_active_processes()
      if active_procs and active_procs[0][1] > 0:
        break
      time.sleep(0.2)
    self.wait_until_running(tm)

    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'ignorant_process'
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
      assert state.processes['ignorant_process'][0].state == ProcessState.LOST
      assert state.processes['ignorant_process'][1].state == ProcessState.KILLED
      assert state.processes['ignorant_process'][2].state == ProcessState.RUNNING
    finally:
      os.kill(state.processes['ignorant_process'][2].coordinator_pid, signal.SIGKILL)
      os.kill(state.processes['ignorant_process'][2].pid, signal.SIGKILL)

  def test_coordinator_dead_kill(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.tempdir, runner.task_id)
    self.wait_until_running(tm)
    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'ignorant_process'
    assert run_number == 0

    os.kill(runner.po.pid, signal.SIGKILL)
    os.kill(process_state.coordinator_pid, signal.SIGKILL)
    os.kill(process_state.pid, signal.SIGKILL)

    killer = TaskRunner.get(runner.task_id, runner.root)
    assert killer is not None
    killer.kill(force=True)

    state = tm.get_state()
    assert len(state.processes['ignorant_process']) == 1
    assert state.processes['ignorant_process'][0].state == ProcessState.LOST

  # TODO(wickman) MESOS-4326
  @pytest.mark.skipif('True')
  def test_preemption_wait(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.tempdir, runner.task_id)
    self.wait_until_running(tm)
    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'ignorant_process'
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
    assert preempter.state.processes['ignorant_process'][-1].state == ProcessState.KILLED


SIMPLEFORK_SCRIPT = """
cat <<EOF | %(INTERPRETER)s -
from __future__ import print_function
import os
import time

pid = os.fork()

if pid == 0:
  pid = os.getpid()
  with open('child.txt', 'w') as fp:
    print(pid, file=fp)
  time.sleep(60)
else:
  with open('parent.txt', 'w') as fp:
    print(os.getpid(), file=fp)
  while not os.path.exists('exit.txt'):
    time.sleep(0.1)
EOF
""" % {'INTERPRETER': sys.executable}


class TestRunnerKillProcessGroup(RunnerBase):
  @classmethod
  def task(cls):
    task = Task(name="task", processes=[Process(name="process", cmdline=SIMPLEFORK_SCRIPT)])
    return task.interpolate()[0]

  def test_pg_is_killed(self):
    runner = self.start_runner()
    tm = TaskMonitor(runner.tempdir, runner.task_id)
    self.wait_until_running(tm)
    process_state, run_number = tm.get_active_processes()[0]
    assert process_state.process == 'process'
    assert run_number == 0

    child_pidfile = os.path.join(runner.sandbox, runner.task_id, 'child.txt')
    while not os.path.exists(child_pidfile):
      time.sleep(0.1)
    parent_pidfile = os.path.join(runner.sandbox, runner.task_id, 'parent.txt')
    while not os.path.exists(parent_pidfile):
      time.sleep(0.1)
    with open(child_pidfile) as fp:
      child_pid = int(fp.read().rstrip())
    with open(parent_pidfile) as fp:
      parent_pid = int(fp.read().rstrip())

    ps = ProcessProviderFactory.get()
    ps.collect_all()
    assert parent_pid in ps.pids()
    assert child_pid in ps.pids()
    assert child_pid in ps.children_of(parent_pid)

    with open(os.path.join(runner.sandbox, runner.task_id, 'exit.txt'), 'w') as fp:
      fp.write('go away!')

    while tm.task_state() is not TaskState.SUCCESS:
      time.sleep(0.1)

    state = tm.get_state()
    assert state.processes['process'][0].state == ProcessState.SUCCESS

    ps.collect_all()
    assert parent_pid not in ps.pids()
    assert child_pid not in ps.pids()
