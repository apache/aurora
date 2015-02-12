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

from mock import call, Mock, patch

from apache.aurora.client.base import AURORA_V2_USER_AGENT_NAME
from apache.aurora.client.cli.context import AuroraCommandContext


def test_get_api_defaults_to_hooks_enabled():
  with patch('apache.aurora.client.cli.context.make_client') as mock_make_client:
    cluster = 'some-cluster'

    AuroraCommandContext().get_api(cluster)
    assert mock_make_client.mock_calls == [call(cluster, AURORA_V2_USER_AGENT_NAME, True)]


def test_get_api_caches_hook_enabled_apis_separately():
  with patch('apache.aurora.client.cli.context.make_client') as mock_make_client:
    # return a new Mock instance for each call of the two calls we're expecting.
    mock_make_client.side_effect = [Mock(), Mock()]

    cluster = 'some-cluster'

    context = AuroraCommandContext()
    hooked_api = context.get_api(cluster)
    unhooked_api = context.get_api(cluster, False)

    assert mock_make_client.mock_calls == [
      call(cluster, AURORA_V2_USER_AGENT_NAME, True),
      call(cluster, AURORA_V2_USER_AGENT_NAME, False)]

    assert hooked_api != unhooked_api

    assert hooked_api in context.apis.values()
    assert hooked_api not in context.unhooked_apis.values()

    assert unhooked_api in context.unhooked_apis.values()
    assert unhooked_api not in context.apis.values()


def test_get_api_forwards_hooks_disabled():
  with patch('apache.aurora.client.cli.context.make_client') as mock_make_client:
    cluster = 'some-cluster'

    AuroraCommandContext().get_api(cluster, enable_hooks=False)
    assert mock_make_client.mock_calls == [call(cluster, AURORA_V2_USER_AGENT_NAME, False)]
