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

import time
import unittest
from contextlib import contextmanager

from mock import Mock, call, patch

from apache.aurora.client.api.sla import JobUpTimeLimit, Sla, task_query
from apache.aurora.client.base import DEFAULT_GROUPING, add_grouping, remove_grouping
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.cluster import Cluster

from gen.apache.aurora.api.constants import LIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    JobKey,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery
)


def rack_grouping(hostname):
  return hostname.split('-')[1]


class SlaTest(unittest.TestCase):
  def setUp(self):
    self._scheduler = Mock()
    self._sla = Sla(self._scheduler)
    self._cluster = Cluster(name='cl')
    self._role = 'mesos'
    self._name = 'job'
    self._env = 'test'
    self._job_key = AuroraJobKey(self._cluster.name, self._role, self._env, self._name)
    self._min_count = 1

  def mock_get_tasks(self, tasks, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, details=[ResponseDetail(message='test')])
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))
    self._scheduler.getTasksWithoutConfigs.return_value = resp

  def create_task(self, duration, id, host=None, name=None, prod=None):
    return ScheduledTask(
        assignedTask=AssignedTask(
            instanceId=id,
            slaveHost=host,
            task=TaskConfig(
                production=prod if prod is not None else True,
                job=JobKey(role=self._role, environment=self._env, name=name or self._name))),
        status=ScheduleStatus.RUNNING,
        taskEvents=[TaskEvent(
            status=ScheduleStatus.RUNNING,
            timestamp=(time.time() - duration) * 1000)]
    )

  def create_tasks(self, durations):
    return [self.create_task(duration, index) for index, duration in enumerate(durations)]

  def assert_count_result(self, percentage, duration):
    vector = self._sla.get_job_uptime_vector(self._job_key)
    actual = vector.get_task_up_count(duration)
    assert percentage == actual, (
        'Expected percentage:%s Actual percentage:%s' % (percentage, actual)
    )
    self.expect_task_status_call_job_scoped()

  def assert_uptime_result(self, expected, percentile):
    vector = self._sla.get_job_uptime_vector(self._job_key)
    try:
      actual = vector.get_job_uptime(percentile)
    except ValueError:
      assert expected is None, 'Unexpected error raised.'
    else:
      assert expected is not None, 'Expected error not raised.'
      assert expected == actual, (
          'Expected uptime:%s Actual uptime:%s' % (expected, actual)
      )
      self.expect_task_status_call_job_scoped()

  def assert_wait_time_result(self, wait_time, percentile, duration, total=None):
    vector = self._sla.get_job_uptime_vector(self._job_key)
    actual = vector.get_wait_time_to_sla(percentile, duration, total)
    assert wait_time == actual, (
        'Expected wait time:%s Actual wait time:%s' % (wait_time, actual)
    )
    self.expect_task_status_call_job_scoped()

  def assert_safe_domain_result(self, host, percentage, duration, in_limit=None, out_limit=None,
      grouping=DEFAULT_GROUPING):
    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count)
    result = vector.get_safe_hosts(percentage, duration, in_limit, grouping)
    assert 1 == len(result), ('Expected length:%s Actual length:%s' % (1, len(result)))
    assert host in result[0], ('Expected host:%s not found in result' % host)
    if out_limit:
      job_details = result[0][host][0]
      assert job_details.job.name == out_limit.job.name, (
          'Expected job:%s Actual:%s' % (out_limit.job.name, job_details.job.name)
      )
      assert job_details.percentage == out_limit.percentage, (
        'Expected %%:%s Actual %%:%s' % (out_limit.percentage, job_details.percentage)
      )
      assert job_details.duration == out_limit.duration, (
        'Expected duration:%s Actual duration:%s' % (out_limit.duration, job_details.duration)
      )
    self.expect_task_status_call_cluster_scoped()

  def assert_probe_hosts_result(self, hosts, percent, duration):
    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count, hosts)
    result = vector.probe_hosts(percent, duration)
    assert len(hosts) == len(result), ('Expected length:%s Actual length:%s' % (1, len(result)))
    return result

  def assert_probe_hosts_result_with_grouping(self, hosts, percent, duration, group_count):
    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count, hosts)
    result = vector.probe_hosts(percent, duration, 'by_rack')
    assert group_count == len(result), ('Expected length:%s Actual length:%s'
        % (group_count, len(result)))
    return result

  def assert_probe_host_job_details(self, result, host, f_percent, safe=True, wait_time=0):
    job_details = None
    for group in result:
      if host in group:
        job_details = group[host][0]
        break

    assert job_details, ('Expected host:%s not found in result' % host)
    assert job_details.job.name == self._name, (
      'Expected job:%s Actual:%s' % (self._name, job_details.job.name)
    )
    assert job_details.predicted_percentage == f_percent, (
      'Expected percentage:%s Actual:%s' % (f_percent, job_details.predicted_percentage)
    )
    assert job_details.safe == safe, (
      'Expected safe:%s Actual:%s' % (safe, job_details.safe)
    )
    assert job_details.safe_in_secs == wait_time, (
      'Expected safe:%s Actual:%s' % (wait_time, job_details.safe_in_secs)
    )

  def expect_task_status_call_job_scoped(self):
    self._scheduler.getTasksWithoutConfigs.assert_called_once_with(TaskQuery(
        jobKeys=[self._job_key.to_thrift()],
        statuses=LIVE_STATES))

  def expect_task_status_call_cluster_scoped(self):
    self._scheduler.getTasksWithoutConfigs.assert_called_with(TaskQuery(statuses=LIVE_STATES))

  @contextmanager
  def group_by_rack(self):
    add_grouping('by_rack', rack_grouping)
    yield
    remove_grouping('by_rack')

  def test_count_0(self):
    self.mock_get_tasks([])
    self.assert_count_result(0, 0)

  def test_count_50(self):
    self.mock_get_tasks(self.create_tasks([600, 900, 100, 200]))
    self.assert_count_result(50, 300)

  def test_count_100(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400, 500]))
    self.assert_count_result(100, 50)

  def test_uptime_empty(self):
    self.mock_get_tasks([])
    self.assert_uptime_result(0, 50)

  def test_uptime_0(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(None, 0)

  def test_uptime_10(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(400, 10)

  def test_uptime_50(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(200, 50)

  def test_uptime_99(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(100, 99)

  def test_uptime_100(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(None, 100)

  def test_wait_time_empty(self):
    self.mock_get_tasks([])
    self.assert_wait_time_result(None, 50, 200)

  def test_wait_time_0(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_wait_time_result(0, 75, 200)

  def test_wait_time_infeasible(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_wait_time_result(None, 95, 200, 5)

  def test_wait_time_upper(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_wait_time_result(50, 25, 450)

  def test_wait_time_mid(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400, 500]))
    self.assert_wait_time_result(50, 50, 350)

  def test_wait_time_lower(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400, 500]))
    self.assert_wait_time_result(50, 90, 150)

  def test_wait_time_with_total(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_wait_time_result(150, 80, 250)

  def test_domain_uptime_no_tasks(self):
    self.mock_get_tasks([])
    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count)
    assert 0 == len(vector.get_safe_hosts(50, 400)), 'Length must be empty.'
    self.expect_task_status_call_cluster_scoped()

  def test_domain_uptime_no_result(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', 'j1'),
        self.create_task(200, 2, 'h2', 'j1')
    ])
    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count)
    assert 0 == len(vector.get_safe_hosts(50, 400)), 'Length must be empty.'
    self.expect_task_status_call_cluster_scoped()

  def test_domain_uptime_no_result_min_count_filtered(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', 'j1'),
        self.create_task(400, 2, 'h2', 'j1'),
        self.create_task(400, 3, 'h3', 'j1'),
        self.create_task(100, 1, 'h2', 'j2')
    ])
    vector = self._sla.get_domain_uptime_vector(self._cluster, 4)
    assert 0 == len(vector.get_safe_hosts(10, 200)), 'Length must be empty.'
    self.expect_task_status_call_cluster_scoped()

  def test_domain_uptime(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', 'j1'),
        self.create_task(200, 2, 'h2', 'j1'),
        self.create_task(100, 1, 'h2', 'j2')
    ])
    self.assert_safe_domain_result('h1', 50, 200)

  def test_domain_uptime_with_override(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', self._name),
        self.create_task(200, 2, 'h2', self._name),
        self.create_task(100, 1, 'h2', 'j2')
    ])

    job_override = {
        self._job_key: JobUpTimeLimit(job=self._job_key, percentage=50, duration_secs=100)
    }
    self.assert_safe_domain_result('h1', 50, 400, in_limit=job_override)

  def test_domain_uptime_not_production(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', self._name, False),
        self.create_task(200, 2, 'h2', self._name, False),
        self.create_task(100, 1, 'h2', self._name, False)
    ])

    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count)
    assert 0 == len(vector.get_safe_hosts(50, 200)), 'Length must be empty.'
    self.expect_task_status_call_cluster_scoped()

  def test_domain_uptime_production_not_set(self):
    task = self.create_task(500, 1, 'h1', self._name)
    task.assignedTask.task.production = None
    self.mock_get_tasks([task])

    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count)
    assert 0 == len(vector.get_safe_hosts(50, 200)), 'Length must be empty.'
    self.expect_task_status_call_cluster_scoped()

  def test_domain_uptime_with_grouping(self):
    with self.group_by_rack():
      self.mock_get_tasks([
          self.create_task(100, 1, 'cl-r1-h01', self._name),
          self.create_task(200, 3, 'cl-r2-h03', self._name),
      ])
      self.assert_safe_domain_result('cl-r1-h01', 50, 150, grouping='by_rack')

  def test_domain_uptime_with_grouping_not_safe(self):
    with self.group_by_rack():
      self.mock_get_tasks([
          self.create_task(200, 1, 'cl-r1-h01', self._name),
          self.create_task(100, 2, 'cl-r1-h02', self._name),
          self.create_task(200, 3, 'cl-r2-h03', self._name),
          self.create_task(100, 4, 'cl-r2-h04', self._name),
      ])
      vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count)
      assert 0 == len(vector.get_safe_hosts(50, 150, None, 'by_rack')), 'Length must be empty.'
      self.expect_task_status_call_cluster_scoped()

  def test_probe_hosts_no_hosts(self):
    self.mock_get_tasks([])
    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count)
    assert 0 == len(vector.probe_hosts(90, 200))

  def test_probe_hosts_no_tasks(self):
    self.mock_get_tasks([], response_code=ResponseCode.INVALID_REQUEST)
    vector = self._sla.get_domain_uptime_vector(self._cluster, self._min_count, hosts=['h1', 'h2'])
    assert 0 == len(vector.probe_hosts(90, 200))

  def test_probe_hosts_no_result(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h3', 'j1'),
        self.create_task(100, 1, 'h4', 'j2')
    ])
    vector = self._sla.get_domain_uptime_vector(self._cluster, ['h1', 'h2'])
    assert 0 == len(vector.probe_hosts(90, 200))

  def test_probe_hosts_no_result_min_count_filtered(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h3', 'j1'),
        self.create_task(100, 1, 'h4', 'j1'),
        self.create_task(100, 1, 'h5', 'j1'),
        self.create_task(100, 1, 'h4', 'j2')
    ])
    vector = self._sla.get_domain_uptime_vector(self._cluster, 4, ['h1'])
    assert 0 == len(vector.probe_hosts(50, 50))

  def test_probe_hosts_safe(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', self._name),
        self.create_task(100, 2, 'h2', self._name),
    ])
    result = self.assert_probe_hosts_result(['h1', 'h2'], 20, 100)
    self.assert_probe_host_job_details(result, 'h1', 50.0)
    self.assert_probe_host_job_details(result, 'h2', 50.0)

  def test_probe_hosts_not_safe(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', self._name),
        self.create_task(200, 2, 'h2', self._name),
        self.create_task(300, 3, 'h3', self._name),
        self.create_task(400, 4, 'h4', self._name),
    ])
    result = self.assert_probe_hosts_result(['h1', 'h2', 'h3', 'h4'], 75, 300)
    self.assert_probe_host_job_details(result, 'h1', 50.0, False, 100)

  def test_probe_hosts_not_safe_infeasible(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', self._name),
        self.create_task(200, 2, 'h2', self._name),
        self.create_task(300, 3, 'h3', self._name),
        self.create_task(400, 4, 'h4', self._name),
    ])
    result = self.assert_probe_hosts_result(['h1', 'h2', 'h3', 'h4'], 80, 300)
    self.assert_probe_host_job_details(result, 'h1', 50.0, False, None)

  def test_probe_hosts_non_prod_ignored(self):
    self.mock_get_tasks([
        self.create_task(100, 1, 'h1', self._name, False),
        self.create_task(200, 2, 'h2', self._name, False),
        self.create_task(300, 3, 'h3', self._name, False),
        self.create_task(400, 4, 'h4', self._name, False),
    ])
    vector = self._sla.get_domain_uptime_vector(self._cluster, ['h1', 'h2'])
    assert 0 == len(vector.probe_hosts(90, 200))

  def test_probe_hosts_with_grouping_safe(self):
    with self.group_by_rack():
      self.mock_get_tasks([
          self.create_task(100, 1, 'cl-r1-h01', self._name),
          self.create_task(100, 3, 'cl-r2-h03', self._name),
      ])
      result = self.assert_probe_hosts_result_with_grouping(
          ['cl-r1-h01', 'cl-r2-h03'], 50, 100, 2)
      self.assert_probe_host_job_details(result, 'cl-r1-h01', 50.0)
      self.assert_probe_host_job_details(result, 'cl-r2-h03', 50.0)

  def test_probe_hosts_with_grouping_not_safe(self):
    with self.group_by_rack():
      self.mock_get_tasks([
          self.create_task(100, 1, 'cl-r1-h01', self._name),
          self.create_task(200, 2, 'cl-r1-h02', self._name),
          self.create_task(100, 3, 'cl-r2-h03', self._name),
          self.create_task(200, 4, 'cl-r2-h04', self._name),
      ])
      result = self.assert_probe_hosts_result_with_grouping(
          ['cl-r1-h01', 'cl-r1-h02', 'cl-r2-h03', 'cl-r2-h04'], 50, 200, 2)
      self.assert_probe_host_job_details(result, 'cl-r1-h01', 25.0, False, 100)
      self.assert_probe_host_job_details(result, 'cl-r1-h02', 25.0, False, 100)
      self.assert_probe_host_job_details(result, 'cl-r2-h03', 25.0, False, 100)
      self.assert_probe_host_job_details(result, 'cl-r2-h04', 25.0, False, 100)

  def test_get_domain_uptime_vector_with_hosts(self):
    with patch('apache.aurora.client.api.sla.task_query', return_value=TaskQuery()) as (mock_query):
      self.mock_get_tasks([
          self.create_task(100, 1, 'h1', 'j1'),
          self.create_task(200, 2, 'h1', 'j2'),
          self.create_task(200, 3, 'h2', 'j1'),
          self.create_task(200, 3, 'h2', 'j3'),
          self.create_task(200, 4, 'h3', 'j4'),
          self.create_task(200, 4, 'h3', 'j3'),
      ])
      hosts = ['h1', 'h2', 'h3']
      jobs = set([
          AuroraJobKey(self._cluster.name, self._role, self._env, 'j1'),
          AuroraJobKey(self._cluster.name, self._role, self._env, 'j2'),
          AuroraJobKey(self._cluster.name, self._role, self._env, 'j3'),
          AuroraJobKey(self._cluster.name, self._role, self._env, 'j4')
      ])

      self._sla.get_domain_uptime_vector(self._cluster, self._min_count, hosts)
      mock_query.assert_has_calls([call(hosts=hosts), call(job_keys=jobs)], any_order=False)

  def test_get_domain_uptime_vector_with_hosts_no_job_tasks(self):
    with patch('apache.aurora.client.api.sla.task_query', return_value=TaskQuery()) as (mock_query):
      self.mock_get_tasks([])

      self._sla.get_domain_uptime_vector(self._cluster, self._min_count, ['h1'])
      mock_query.assert_called_once_with(hosts=['h1'])

  def test_task_query(self):
    jobs = set([
        AuroraJobKey(self._cluster.name, self._role, self._env, 'j1'),
        AuroraJobKey(self._cluster.name, self._role, self._env, 'j2'),
        AuroraJobKey(self._cluster.name, self._role, self._env, 'j3'),
        AuroraJobKey(self._cluster.name, self._role, self._env, 'j4')
    ])

    query = task_query(job_keys=jobs)
    assert len(jobs) == len(query.jobKeys), 'Expected length:%s, Actual:%s' % (
        len(jobs), len(query.jobKeys)
    )
    assert LIVE_STATES == query.statuses, 'Expected:%s, Actual:%s' % (
      LIVE_STATES, query.statuses
    )
