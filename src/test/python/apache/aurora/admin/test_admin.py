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

import contextlib

from mock import PropertyMock, call, create_autospec, patch

from apache.aurora.admin.admin import (
    get_scheduler,
    increase_quota,
    prune_tasks,
    query,
    reconcile_tasks,
    set_quota
)
from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.api.scheduler_client import SchedulerClient, SchedulerProxy

from .util import AuroraClientCommandTest

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    GetQuotaResult,
    JobKey,
    ResourceAggregate,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskQuery
)


class TestPruneCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_options(cls, states=None, role=None, env=None, limit=None):
    mock_options = create_autospec(
        spec=['states', 'role', 'environment', 'limit'],
        spec_set=False,
        instance=True)

    mock_options.role = role
    mock_options.states = states
    mock_options.environment = env
    mock_options.limit = limit
    mock_options.bypass_leader_redirect = False

    return mock_options

  @classmethod
  def task_query(cls, options):
    query = TaskQuery(
        role=options.role,
        environment=options.environment,
        limit=options.limit)
    if options.states:
      query.statuses = set(map(ScheduleStatus._NAMES_TO_VALUES.get, options.states.split(',')))
    return query

  def test_prune(self):
    mock_options = self.setup_mock_options(
      role='aurora', env='devel', limit=10, states="LOST,FINISHED")
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
            return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (_, mock_make_admin_client, _):
      api = mock_make_admin_client.return_value
      api.prune_tasks.return_value = Response(responseCode=ResponseCode.OK)

      prune_tasks(['cluster'], mock_options)

      api.prune_tasks.assert_called_once_with(self.task_query(mock_options))


class TestQueryCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls, force=False, shards=None, states='RUNNING', listformat=None):
    mock_options = create_autospec(
        spec=['force', 'shards', 'states', 'listformat', 'verbosity'],
        spec_set=False,
        instance=True)
    mock_options.force = force
    mock_options.shards = shards
    mock_options.states = states
    mock_options.listformat = listformat or '%role%/%name%/%instanceId% %status%'
    mock_options.verbosity = False
    mock_options.bypass_leader_redirect = False
    return mock_options

  @classmethod
  def create_response(cls, tasks, response_code=ResponseCode.OK):
    return Response(
      responseCode=response_code,
      details=[ResponseDetail(message='test')],
      result=Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks)))

  @classmethod
  def create_task(cls):
    return [ScheduledTask(
        assignedTask=AssignedTask(
            instanceId=0,
            task=TaskConfig(job=JobKey(role='role', environment='test', name='job'))
        ),
        status=ScheduleStatus.RUNNING
    )]

  @classmethod
  def task_query(cls):
    return TaskQuery(
        role='test_role',
        jobName='test_job',
        instanceIds=set([0]),
        statuses=set([ScheduleStatus.RUNNING]))

  def test_query(self):
    """Tests successful execution of the query command."""
    mock_options = self.setup_mock_options(force=True, shards="0")
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
            return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (_, mock_make_admin_client, _):

      api = mock_make_admin_client.return_value
      api.query.return_value = self.create_response(self.create_task())

      query([self.TEST_CLUSTER, 'test_role', 'test_job'], mock_options)

      api.query.assert_called_with(self.task_query())

  def test_query_fails(self):
    """Tests failed execution of the query command."""
    mock_options = self.setup_mock_options(shards="0")
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
            return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (_, mock_make_admin_client, _):

      api = mock_make_admin_client.return_value
      api.query.return_value = self.create_response(self.create_task())

      try:
        query([self.TEST_CLUSTER, 'test_role', 'test_job'], mock_options)
      except SystemExit:
        pass
      else:
        assert 'Expected exception is not raised'

      api.query.assert_called_with(self.task_query())


class TestIncreaseQuotaCommand(AuroraClientCommandTest):

  @classmethod
  def create_response(cls, quota, prod, non_prod, response_code=ResponseCode.OK):
    return Response(
        responseCode=response_code,
        details=[ResponseDetail(message='test')],
        result=Result(getQuotaResult=GetQuotaResult(
            quota=quota, prodSharedConsumption=prod, nonProdSharedConsumption=non_prod))
    )

  def test_increase_quota(self):
    """Tests successful execution of the increase_quota command."""
    mock_options = self.setup_mock_options()
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
            return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (_, mock_make_admin_client, _):

      api = mock_make_admin_client.return_value
      role = 'test_role'
      api.get_quota.return_value = self.create_response(
          ResourceAggregate(20.0, 4000, 6000),
          ResourceAggregate(15.0, 2000, 3000),
          ResourceAggregate(6.0, 200, 600),
      )
      api.set_quota.return_value = self.create_simple_success_response()

      increase_quota([self.TEST_CLUSTER, role, '4.0', '1MB', '1MB'])

      api.set_quota.assert_called_with(role, 24.0, 4001, 6001)
      assert isinstance(api.set_quota.call_args[0][1], float)
      assert isinstance(api.set_quota.call_args[0][2], int)
      assert isinstance(api.set_quota.call_args[0][3], int)


class TestSetQuotaCommand(AuroraClientCommandTest):

  @classmethod
  def create_response(cls, quota, prod, non_prod, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, details=[ResponseDetail(message='test')])
    resp.result = Result(getQuotaResult=GetQuotaResult(
      quota=quota, prodSharedConsumption=prod, nonProdSharedConsumption=non_prod))
    return resp

  def test_set_quota(self):
    """Tests successful execution of the set_quota command."""
    mock_options = self.setup_mock_options()
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
              return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (_, mock_make_admin_client, _):

      api = mock_make_admin_client.return_value
      role = 'test_role'
      api.set_quota.return_value = self.create_simple_success_response()

      set_quota([self.TEST_CLUSTER, role, '4.0', '10MB', '10MB'])

      api.set_quota.assert_called_with(role, 4.0, 10, 10)
      assert isinstance(api.set_quota.call_args[0][1], float)
      assert isinstance(api.set_quota.call_args[0][2], int)
      assert isinstance(api.set_quota.call_args[0][3], int)


class TestGetSchedulerCommand(AuroraClientCommandTest):

  def test_get_scheduler(self):
    """Tests successful execution of the get_scheduler command."""
    mock_options = self.setup_mock_options()
    mock_proxy = create_autospec(spec=SchedulerProxy, instance=True)
    mock_scheduler_client = create_autospec(spec=SchedulerClient, instance=True)
    mock_raw_url = PropertyMock(return_value="url")
    mock_proxy.scheduler_client.return_value = mock_scheduler_client
    type(mock_scheduler_client).raw_url = mock_raw_url

    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
              return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS),
    ) as (_, mock_make_admin_client, _):

      api = mock_make_admin_client.return_value
      type(api).scheduler_proxy = PropertyMock(return_value=mock_proxy)

      get_scheduler([self.TEST_CLUSTER])

      mock_raw_url.assert_called_once_with()


class TestReconcileTaskCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls, reconcile_type='explicit', batch_size=None):
    mock_options = create_autospec(spec=['type', 'batch_size'], instance=True)
    mock_options.type = reconcile_type
    mock_options.batch_size = batch_size
    mock_options.bypass_leader_redirect = False
    return mock_options

  def test_reconcile_implicit(self):
    """Tests successful execution of the reconcile_tasks command."""
    mock_options = self.setup_mock_options(reconcile_type='implicit')
    mock_proxy = create_autospec(spec=SchedulerProxy, instance=True)
    mock_scheduler_client = create_autospec(spec=SchedulerClient)
    mock_proxy.scheduler_client.return_value = mock_scheduler_client

    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
              return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS),
    ) as (_, mock_make_admin_client, _):

      api = mock_make_admin_client.return_value
      type(api).scheduler_proxy = PropertyMock(return_value=mock_proxy)
      api.reconcile_implicit.return_value = self.create_simple_success_response()
      reconcile_tasks([self.TEST_CLUSTER])
      assert api.reconcile_implicit.mock_calls == [call()]

  def test_reconcile_explicit(self):
    """Tests successful execution of the reconcile_tasks command."""
    mock_options = self.setup_mock_options()
    mock_proxy = create_autospec(spec=SchedulerProxy, instance=True)
    mock_scheduler_client = create_autospec(spec=SchedulerClient, instance=True)
    mock_proxy.scheduler_client.return_value = mock_scheduler_client

    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
              return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS),
    ) as (_, mock_make_admin_client, _):

      api = mock_make_admin_client.return_value
      type(api).scheduler_proxy = PropertyMock(return_value=mock_proxy)
      api.reconcile_explicit.return_value = self.create_simple_success_response()
      reconcile_tasks([self.TEST_CLUSTER])
      assert api.reconcile_explicit.mock_calls == [call(None)]

  def test_reconcile_explicit_batch_size(self):
    """Tests successful execution of the reconcile_tasks command."""
    mock_options = self.setup_mock_options(batch_size=500)
    mock_proxy = create_autospec(spec=SchedulerProxy, instance=True)
    mock_scheduler_client = create_autospec(spec=SchedulerClient, instance=True)
    mock_proxy.scheduler_client.return_value = mock_scheduler_client

    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.admin.admin.make_admin_client',
              return_value=create_autospec(spec=AuroraClientAPI)),
        patch('apache.aurora.admin.admin.CLUSTERS', new=self.TEST_CLUSTERS),
    ) as (_, mock_make_admin_client, _):

      api = mock_make_admin_client.return_value
      type(api).scheduler_proxy = PropertyMock(return_value=mock_proxy)
      api.reconcile_explicit.return_value = self.create_simple_success_response()
      reconcile_tasks([self.TEST_CLUSTER])
      assert api.reconcile_explicit.mock_calls == [call(500)]
