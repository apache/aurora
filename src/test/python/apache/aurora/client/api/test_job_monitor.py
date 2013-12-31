from gen.apache.aurora.AuroraSchedulerManager import Client
from gen.apache.aurora.ttypes import Response, ResponseCode, Result, ScheduleStatusResult, Identity, TaskQuery
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
    self.mock_api.scheduler = self.mock_scheduler

  def test_init(self):
    result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=[]))
    response = Response(responseCode=ResponseCode.OK, message="test", result=result)
    query = TaskQuery(owner=Identity(role=ROLE), environment=ENV, jobName=JOB_NAME)

    self.mock_scheduler.getTasksStatus(query).AndReturn(response)

    self.mox.ReplayAll()

    JobMonitor(self.mock_api, ROLE, ENV, JOB_NAME)
