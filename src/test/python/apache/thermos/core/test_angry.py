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

import random

from apache.thermos.config.schema import Process, Task
from apache.thermos.testing.runner import Runner

from gen.apache.thermos.ttypes import ProcessState, TaskState


def flaky_task():
  task = Task(
    name = "failing_task",
    max_failures = 2,
    processes = [
      Process(name = "a", max_failures=1, min_duration=1, cmdline="echo hello world"),
      Process(name = "b", max_failures=2, min_duration=1, cmdline="exit 1"),
      Process(name = "c", max_failures=1, min_duration=1, final=True, cmdline="echo hello world")
    ],
    constraints = [{'order': ['a', 'b']}]
  )
  return task.interpolate()[0]


def test_flaky_runner():
  runner = Runner(flaky_task(), success_rate=90, random_seed=31337)

  count = 0
  while True:
    count += 1
    print('Run #%d' % count)
    rc = runner.run()
    if rc == 0:
      break

  print('Completed in %d runs' % count)
  assert runner.state.statuses[-1].state == TaskState.SUCCESS
  assert runner.state.processes['a'][-1].state == ProcessState.SUCCESS
  assert runner.state.processes['b'][-1].state == ProcessState.FAILED
  assert runner.state.processes['c'][-1].state == ProcessState.SUCCESS
