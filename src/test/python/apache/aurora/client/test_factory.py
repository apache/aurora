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

from mock import patch

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.factory import make_client
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.client.util import TEST_CLUSTER, TEST_CLUSTERS


def test_make_client_defaults_to_hooks_enabled():
  with patch('apache.aurora.client.factory.CLUSTERS', new=TEST_CLUSTERS):
    assert isinstance(make_client(TEST_CLUSTER, 'some-user-agent'), HookedAuroraClientAPI)


def test_make_client_hooks_disabled():
  with patch('apache.aurora.client.factory.CLUSTERS', new=TEST_CLUSTERS):
    client = make_client(TEST_CLUSTER, 'some-user-agent', enable_hooks=False)
    assert not isinstance(client, HookedAuroraClientAPI)
    assert isinstance(client, AuroraClientAPI)
