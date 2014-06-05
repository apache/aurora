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

from apache.thermos.config.schema import Process, Task
from apache.thermos.testing.runner import RunnerTestBase

from gen.apache.thermos.ttypes import ProcessState, TaskState


class TestEphemeralTask(RunnerTestBase):
  @classmethod
  def task(cls):
    task = Task(
        name="task_with_ephemeral",
        processes=[
          Process(name="ephemeral_sleepy", ephemeral=True, cmdline="sleep 10"),
          Process(name="sleepy", cmdline="sleep 1")
        ])
    return task.interpolate()[0]

  def test_runner_state(self):
    assert self.state.statuses[-1].state == TaskState.SUCCESS

  def test_runner_process_in_expected_states(self):
    processes = self.state.processes
    # sleepy succeeded
    assert len(processes['sleepy']) == 1
    assert processes['sleepy'][0].state == ProcessState.SUCCESS
    # ephemeral sleepy was terminated because sleepy finished and it's ephemeral
    assert len(processes['ephemeral_sleepy']) == 1
    assert processes['ephemeral_sleepy'][0].state == ProcessState.KILLED
