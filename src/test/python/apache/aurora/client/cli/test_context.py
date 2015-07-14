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
import mock
import pytest

from apache.aurora.client.api import AuroraClientAPI, SchedulerProxy
from apache.aurora.client.cli import EXIT_AUTH_ERROR, Context
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import CLUSTERS

from ...api_util import SchedulerProxyApiSpec

TEST_CLUSTER = Cluster(name='some-cluster', auth_mechanism='nothing', scheduler_uri='nowhere')


def test_get_api_defaults_to_hooks_enabled():
  with CLUSTERS.patch([TEST_CLUSTER]):
    api = AuroraCommandContext().get_api(TEST_CLUSTER.name)
    assert isinstance(api, HookedAuroraClientAPI)
    assert api._cluster == TEST_CLUSTER


def test_get_api_forwards_hooks_disabled():
  with CLUSTERS.patch([TEST_CLUSTER]):
    api = AuroraCommandContext().get_api(TEST_CLUSTER.name, enable_hooks=False)
    assert isinstance(api, AuroraClientAPI)
    assert api._cluster == TEST_CLUSTER


def test_get_api_caches_hook_enabled_apis_separately():
  with CLUSTERS.patch([TEST_CLUSTER]):
    context = AuroraCommandContext()
    hooked_api = context.get_api(TEST_CLUSTER.name)
    unhooked_api = context.get_api(TEST_CLUSTER.name, enable_hooks=False)

    assert hooked_api != unhooked_api

    assert hooked_api in context.apis.values()
    assert hooked_api not in context.unhooked_apis.values()

    assert unhooked_api in context.unhooked_apis.values()
    assert unhooked_api not in context.apis.values()


def test_handles_api_auth_error():
  context = AuroraCommandContext()

  mock_scheduler_proxy = mock.create_autospec(spec=SchedulerProxyApiSpec, instance=True)
  mock_scheduler_proxy.killTasks.side_effect = SchedulerProxy.AuthError()

  mock_api = AuroraClientAPI(TEST_CLUSTER, 'user-agent')
  mock_api._scheduler_proxy = mock_scheduler_proxy

  context.apis = {
    TEST_CLUSTER.name: mock_api
  }
  api = context.get_api(TEST_CLUSTER.name, clusters={TEST_CLUSTER.name: TEST_CLUSTER})

  with pytest.raises(Context.CommandError) as e:
    api.kill_job(AuroraJobKey(TEST_CLUSTER.name, 'role', 'env', 'job'))

  assert e.value.code == EXIT_AUTH_ERROR
  assert mock_scheduler_proxy.killTasks.call_count == 1
