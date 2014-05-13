#
# Copyright 2013 Apache Software Foundation
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
from textwrap import dedent

import pytest

from apache.thermos.config.schema import order, Process, Resources, SequentialTask, Task, Tasks
from apache.thermos.testing.runner import RunnerTestBase

from gen.apache.thermos.ttypes import ProcessState, TaskState


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
    tsk = Task(name = "complex", processes = [t1, t2, t3, t4, t5, t6])
    # three ways of tasks: t1 t2, t3 t4, t5 t6
    tsk = tsk(constraints = [{'order': ['t1', 't3']},
                             {'order': ['t1', 't4']},
                             {'order': ['t2', 't3']},
                             {'order': ['t2', 't4']},
                             {'order': ['t3', 't5']},
                             {'order': ['t3', 't6']},
                             {'order': ['t4', 't5']},
                             {'order': ['t4', 't6']}])
    return tsk

  def test_runner_state_success(self):
    assert self.state.statuses[-1].state == TaskState.SUCCESS

  def test_runner_header_populated(self):
    header = self.state.header
    assert header is not None, 'header should be populated.'
    assert header.task_id == self.runner.task_id, 'header task id must be set!'
    assert header.sandbox == os.path.join(self.runner.tempdir, 'sandbox', header.task_id), (
      'header sandbox must be set!')
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
          assert history[run].state != ProcessState.SUCCESS, (
            "nonterminal processes must not be in SUCCESS state!")

  def test_runner_processes_have_monotonically_increasing_timestamps(self):
    for process in self.state.processes:
      for run in self.state.processes[process]:
        assert run.fork_time < run.start_time
        assert run.start_time < run.stop_time


class TestConcurrencyBasic(RunnerTestBase):
  @classmethod
  def task(cls):
    hello_template = Process(cmdline = "sleep 1")
    tsk = Task(
      name = "complex",
      processes = [hello_template(name = "process1"),
                   hello_template(name = "process2"),
                   hello_template(name = "process3")],
      resources = Resources(cpu = 1.0, ram = 16*1024*1024, disk = 16*1024),
      max_concurrency = 1)
    return tsk

  def test_runner_state_success(self):
    assert self.state.statuses[-1].state == TaskState.SUCCESS

  # TODO(wickman)  This needs a better test.
  def test_runner_processes_separated_temporally_due_to_concurrency_limit(self):
    runs = []
    for process in self.state.processes:
      assert len(self.state.processes[process]) == 1, 'Expect one run per task'
      assert self.state.processes[process][0].state == ProcessState.SUCCESS
      runs.append(self.state.processes[process][0].start_time)
    runs.sort()
    assert runs[1] - runs[0] > 1.0
    assert runs[2] - runs[1] > 1.0


class TestRunnerEnvironment(RunnerTestBase):
  @classmethod
  def task(cls):
    setup_bashrc = Process(name = "setup_bashrc", cmdline = dedent(
    """
    mkdir -p .profile.d
    cat <<EOF > .thermos_profile
    for i in .profile.d/*.sh ; do
      if [ -r "\\$i" ]; then
        . \\$i
      fi
    done
    EOF
    """))

    setup_foo = Process(name = "setup_foo", cmdline = dedent(
    """
    cat <<EOF > .profile.d/setup_foo.sh
    export FOO=1
    EOF
    """))

    setup_bar = Process(name = "setup_bar", cmdline = dedent(
    """
    cat <<EOF > .profile.d/setup_bar.sh
    export BAR=2
    EOF
    """))

    foo_recipe = SequentialTask(processes = [setup_bashrc, setup_foo])
    bar_recipe = SequentialTask(processes = [setup_bashrc, setup_bar])
    all_recipes = Tasks.combine(foo_recipe, bar_recipe)

    run = Process(name = "run", cmdline = dedent(
    """
    echo $FOO $BAR > expected_output.txt
    """
    ))

    my_task = Task(processes = [run],
                   resources = Resources(cpu = 1.0, ram = 16*1024*1024, disk = 16*1024))
    return Tasks.concat(all_recipes, my_task, name = "my_task")

  def test_runner_state_success(self):
    assert self.state.statuses[-1].state == TaskState.SUCCESS

  def test_runner_processes_have_expected_output(self):
    expected_output_file = os.path.join(self.runner.sandbox, self.runner.task_id,
                                        'expected_output.txt')
    assert os.path.exists(expected_output_file)
    with open(expected_output_file, 'rb') as fp:
      assert fp.read().strip() == b"1 2"
