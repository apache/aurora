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

from mox import IgnoreArg, MoxTestBase

from apache.aurora.client.api.instance_watcher import InstanceWatcher
from apache.aurora.client.api.restarter import Restarter, RestartSettings
from apache.aurora.common.aurora_job_key import AuroraJobKey

from ...api_util import SchedulerProxyApiSpec

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    ServerInfo,
    TaskConfig
)

CLUSTER = 'east'
JOB = AuroraJobKey(CLUSTER, 'johndoe', 'test', 'test_job')
RESTART_SETTINGS = RestartSettings(
    batch_size=2,
    watch_secs=45,
    max_per_instance_failures=0,
    max_total_failures=0,
    health_check_interval_seconds=5)


def make_response(code=ResponseCode.OK, message='test', result=None):
  return Response(
    responseCode=code,
    details=[ResponseDetail(message=message)],
    result=result,
    serverInfo=ServerInfo(clusterName='test'))


class TestRestarter(MoxTestBase):

  def setUp(self):
    super(TestRestarter, self).setUp()

    self.mock_instance_watcher = self.mox.CreateMock(InstanceWatcher)
    self.mock_scheduler_proxy = self.mox.CreateMock(SchedulerProxyApiSpec)

    self.restarter = Restarter(
        JOB,
        RESTART_SETTINGS,
        self.mock_scheduler_proxy,
        self.mock_instance_watcher)

  def mock_restart_instances(self, instances):
    self.mock_scheduler_proxy.restartShards(JOB.to_thrift(), instances, retry=True).AndReturn(
      make_response())
    self.mock_instance_watcher.watch(instances).AndReturn([])

  def test_restart_one_iteration(self):
    self.mock_status_active_tasks([0, 1, 3, 4, 5])
    self.mock_restart_instances([0, 1])

    self.mox.ReplayAll()

    self.restarter.restart([0, 1])

  def mock_three_iterations(self):
    self.mock_restart_instances([0, 1])
    self.mock_restart_instances([3, 4])
    self.mock_restart_instances([5])

  def test_rolling_restart(self):
    self.mock_status_active_tasks([0, 1, 3, 4, 5])
    self.mock_three_iterations()

    self.mox.ReplayAll()

    self.restarter.restart([0, 1, 3, 4, 5])

  def mock_status_active_tasks(self, instance_ids):
    tasks = []
    for i in instance_ids:
      tasks.append(ScheduledTask(
          status=ScheduleStatus.RUNNING,
          assignedTask=AssignedTask(task=TaskConfig(), instanceId=i)
      ))
    response = make_response(result=Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks)))
    self.mock_scheduler_proxy.getTasksWithoutConfigs(IgnoreArg(), retry=True).AndReturn(response)

  def test_restart_all_instances(self):
    self.mock_status_active_tasks([0, 1, 3, 4, 5])
    self.mock_three_iterations()

    self.mox.ReplayAll()

    self.restarter.restart(None)

  def mock_status_no_active_task(self):
    response = make_response(code=ResponseCode.INVALID_REQUEST)
    self.mock_scheduler_proxy.getTasksWithoutConfigs(IgnoreArg(), retry=True).AndReturn(response)

  def test_restart_no_instance_active(self):
    self.mock_status_no_active_task()

    self.mox.ReplayAll()

    self.restarter.restart(None)

  def mock_restart_fails(self):
    response = make_response(code=ResponseCode.ERROR, message='test error')
    self.mock_scheduler_proxy.restartShards(JOB.to_thrift(), IgnoreArg(), retry=True).AndReturn(
      response)

  def test_restart_instance_fails(self):
    self.mock_status_active_tasks([0, 1])
    self.mock_restart_fails()

    self.mox.ReplayAll()

    assert self.restarter.restart(None).responseCode == ResponseCode.ERROR

  def mock_restart_watch_fails(self, instances):
    self.mock_scheduler_proxy.restartShards(JOB.to_thrift(), instances, retry=True).AndReturn(
      make_response())
    self.mock_instance_watcher.watch(instances).AndReturn(instances)

  def test_restart_instances_watch_fails(self):
    instances = [0, 1]
    self.mock_status_active_tasks(instances)
    self.mock_restart_watch_fails(instances)

    self.mox.ReplayAll()

    self.restarter.restart(None)
