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

import unittest
from copy import deepcopy

from mock import create_autospec

from apache.aurora.client.api.quota_check import CapacityRequest, QuotaCheck

from .api_util import SchedulerThriftApiSpec

from gen.apache.aurora.api.ttypes import (
    GetQuotaResult,
    JobKey,
    ResourceAggregate,
    Response,
    ResponseCode,
    ResponseDetail,
    Result
)


class QuotaCheckTest(unittest.TestCase):
  def setUp(self):
    self._scheduler = create_autospec(spec=SchedulerThriftApiSpec, instance=True)
    self._quota_checker = QuotaCheck(self._scheduler)
    self._role = 'mesos'
    self._name = 'quotajob'
    self._env = 'test'
    self._job_key = JobKey(name=self._name, environment=self._env, role=self._role)

  def mock_get_quota(self, allocated, consumed, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code

    resp = Response(responseCode=response_code, details=[ResponseDetail(message='test')])
    resp.result = Result(
        getQuotaResult=GetQuotaResult(
          quota=deepcopy(allocated), prodConsumption=deepcopy(consumed)))
    self._scheduler.getQuota.return_value = resp

  def assert_result(self, prod, released, acquired, expected_code=None):
    expected_code = ResponseCode.OK if expected_code is None else expected_code
    resp = self._quota_checker.validate_quota_from_requested(
        self._job_key,
        prod,
        released,
        acquired)
    assert expected_code == resp.responseCode, (
      'Expected response:%s Actual response:%s' % (expected_code, resp.responseCode))
    if prod:
      self._scheduler.getQuota.assert_called_once_with(self._role)
    else:
      assert not self._scheduler.getQuota.called, 'Scheduler.getQuota() unexpected call.'

  def test_pass(self):
    allocated = ResourceAggregate(numCpus=50.0, ramMb=1000, diskMb=3000)
    consumed = ResourceAggregate(numCpus=25.0, ramMb=500, diskMb=2000)
    released = CapacityRequest(ResourceAggregate(numCpus=5.0, ramMb=100, diskMb=500))
    acquired = CapacityRequest(ResourceAggregate(numCpus=15.0, ramMb=300, diskMb=800))

    self.mock_get_quota(allocated, consumed)
    self.assert_result(True, released, acquired)

  def test_pass_with_no_consumed(self):
    allocated = ResourceAggregate(numCpus=50.0, ramMb=1000, diskMb=3000)
    released = CapacityRequest(ResourceAggregate(numCpus=5.0, ramMb=100, diskMb=500))
    acquired = CapacityRequest(ResourceAggregate(numCpus=15.0, ramMb=300, diskMb=800))

    self.mock_get_quota(allocated, None)
    self.assert_result(True, released, acquired)

  def test_pass_due_to_released(self):
    allocated = ResourceAggregate(numCpus=50.0, ramMb=1000, diskMb=3000)
    consumed = ResourceAggregate(numCpus=45.0, ramMb=900, diskMb=2900)
    released = CapacityRequest(ResourceAggregate(numCpus=5.0, ramMb=100, diskMb=100))
    acquired = CapacityRequest(ResourceAggregate(numCpus=10.0, ramMb=200, diskMb=200))

    self.mock_get_quota(allocated, consumed)
    self.assert_result(True, released, acquired)

  def test_skipped(self):
    self.assert_result(False, None, None)

  def test_fail(self):
    allocated = ResourceAggregate(numCpus=50.0, ramMb=1000, diskMb=3000)
    consumed = ResourceAggregate(numCpus=25.0, ramMb=500, diskMb=2000)
    released = CapacityRequest(ResourceAggregate(numCpus=5.0, ramMb=100, diskMb=500))
    acquired = CapacityRequest(ResourceAggregate(numCpus=35.0, ramMb=300, diskMb=800))

    self.mock_get_quota(allocated, consumed)
    self.assert_result(True, released, acquired, ResponseCode.INVALID_REQUEST)

  def test_fail_scheduler_call(self):
    allocated = ResourceAggregate(numCpus=50.0, ramMb=1000, diskMb=3000)
    consumed = ResourceAggregate(numCpus=25.0, ramMb=500, diskMb=2000)
    released = CapacityRequest(ResourceAggregate(numCpus=5.0, ramMb=100, diskMb=500))
    acquired = CapacityRequest(ResourceAggregate(numCpus=1.0, ramMb=100, diskMb=100))

    self.mock_get_quota(allocated, consumed, response_code=ResponseCode.INVALID_REQUEST)
    self.assert_result(True, released, acquired, ResponseCode.INVALID_REQUEST)
