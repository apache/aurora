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

from inspect import getargspec

from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI, NonHookedAuroraClientAPI
from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.common.cluster import Cluster

from mock import Mock


API_METHODS = ('cancel_update', 'create_job', 'kill_job', 'restart', 'start_cronjob', 'update_job')
API_METHODS_WITH_CONFIG_PARAM_ADDED = ('cancel_update', 'kill_job', 'restart', 'start_cronjob')


def pytest_generate_tests(metafunc):
  if 'method_name' in metafunc.funcargnames:
    metafunc.parametrize('method_name', API_METHODS)


def test_api_methods_exist(method_name):
  api = Mock(spec=AuroraClientAPI)
  method = getattr(api, method_name)
  method()  # is callable
  method.assert_called_once_with()


def test_api_methods_params(method_name):
  cluster = Mock(spec=Cluster)
  api = HookedAuroraClientAPI(cluster=cluster)  # cant use mock here; need to inspect methods

  hooked_method = getattr(api, method_name)
  nonhooked_method = getattr(super(HookedAuroraClientAPI, api), method_name)
  api_method = getattr(super(NonHookedAuroraClientAPI, api), method_name)

  if method_name in API_METHODS_WITH_CONFIG_PARAM_ADDED:
    assert api_method != nonhooked_method
  assert nonhooked_method != hooked_method

  api_argspec = getargspec(api_method)
  hooked_argspec = getargspec(hooked_method)
  nonhooked_argspec = getargspec(nonhooked_method)

  if method_name in API_METHODS_WITH_CONFIG_PARAM_ADDED:
    assert api_argspec.varargs == nonhooked_argspec.varargs
    assert api_argspec.keywords == nonhooked_argspec.keywords
    assert len(api_argspec.args) + 1 == len(nonhooked_argspec.args)
    assert nonhooked_argspec.args[len(api_argspec.args)] == 'config'
    if api_argspec.defaults is None:
      assert len(nonhooked_argspec.defaults) == 1
      assert nonhooked_argspec.defaults[0] is None
    else:
      assert len(api_argspec.defaults) + 1 == len(nonhooked_argspec.defaults)
      assert nonhooked_argspec.defaults[len(api_argspec.defaults)] is None
  else:
    assert nonhooked_argspec == hooked_argspec
  assert nonhooked_argspec == nonhooked_argspec


