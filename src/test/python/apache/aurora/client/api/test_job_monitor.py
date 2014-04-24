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

from gen.apache.aurora.api.AuroraSchedulerManager import Client
from gen.apache.aurora.api.ttypes import (
    Identity,
    Response,
    ResponseCode,
    Result,
    ScheduleStatusResult,
    TaskQuery,
)
from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.api.job_monitor import JobMonitor

from mox import MoxTestBase

ROLE = 'johndoe'
ENV = 'test'
JOB_NAME = 'test_job'


class JobMonitorTest(MoxTestBase):

  def setUp(self):

    super(JobMonitorTest, self).setUp()
    self.mock_api = self.mox.CreateMock(AuroraClientAPI)
    self.mock_scheduler = self.mox.CreateMock(Client)
    self.mock_api.scheduler_proxy = self.mock_scheduler

  def test_init(self):
    result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=[]))
    response = Response(responseCode=ResponseCode.OK, message="test", result=result)
    query = TaskQuery(owner=Identity(role=ROLE), environment=ENV, jobName=JOB_NAME)

    self.mock_scheduler.getTasksStatus(query).AndReturn(response)

    self.mox.ReplayAll()

    JobMonitor(self.mock_api, ROLE, ENV, JOB_NAME)
