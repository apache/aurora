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

from contextlib import contextmanager

import mock

from apache.aurora.config.schema.base import HttpLifecycleConfig, LifecycleConfig, MesosTaskInstance
from apache.aurora.executor.http_lifecycle import HttpLifecycleManager
from apache.aurora.executor.thermos_task_runner import ThermosTaskRunner


def test_http_lifecycle_wrapper_without_lifecycle():
  mti_without_lifecycle = MesosTaskInstance()
  mti_without_http_lifecycle = MesosTaskInstance(lifecycle=LifecycleConfig())

  runner_mock = mock.create_autospec(ThermosTaskRunner)
  assert HttpLifecycleManager.wrap(runner_mock, mti_without_lifecycle, {}) is runner_mock
  assert HttpLifecycleManager.wrap(runner_mock, mti_without_http_lifecycle, {}) is runner_mock


@contextmanager
def make_mocks(mesos_task_instance, portmap):
  # wrap it once health is available and validate the constructor is called as expected
  runner_mock = mock.create_autospec(ThermosTaskRunner)
  with mock.patch.object(HttpLifecycleManager, '__init__', return_value=None) as wrapper_init:
    runner_wrapper = HttpLifecycleManager.wrap(runner_mock, mesos_task_instance, portmap)
    yield runner_mock, runner_wrapper, wrapper_init


def test_http_lifecycle_wrapper_with_lifecycle():
  runner_mock = mock.create_autospec(ThermosTaskRunner)
  mti = MesosTaskInstance(lifecycle=LifecycleConfig(http=HttpLifecycleConfig()))

  # pass-thru if no health port has been defined -- see comment in http_lifecycle.py regarding
  # the rationalization for this behavior.
  with make_mocks(mti, {}) as (runner_mock, runner_wrapper, wrapper_init):
    assert runner_mock is runner_wrapper
    assert wrapper_init.mock_calls == []

  # wrap it once health is available and validate the constructor is called as expected
  with make_mocks(mti, {'health': 31337}) as (runner_mock, runner_wrapper, wrapper_init):
    assert isinstance(runner_wrapper, HttpLifecycleManager)
    assert wrapper_init.mock_calls == [
      mock.call(runner_mock, 31337, ['/quitquitquit', '/abortabortabort'])
    ]

  # Validate that we can override ports
  mti = MesosTaskInstance(lifecycle=LifecycleConfig(http=HttpLifecycleConfig(
      port='http',
      graceful_shutdown_endpoint='/frankfrankfrank',
      shutdown_endpoint='/bobbobbob',
  )))
  portmap = {'http': 12345, 'admin': 54321}
  with make_mocks(mti, portmap) as (runner_mock, runner_wrapper, wrapper_init):
    assert isinstance(runner_wrapper, HttpLifecycleManager)
    assert wrapper_init.mock_calls == [
      mock.call(runner_mock, 12345, ['/frankfrankfrank', '/bobbobbob'])
    ]


def test_http_lifecycle_wraps_start_and_stop():
  mti = MesosTaskInstance(lifecycle=LifecycleConfig(http=HttpLifecycleConfig()))
  runner_mock = mock.create_autospec(ThermosTaskRunner)
  with mock.patch.object(HttpLifecycleManager, '_terminate_http', return_value=None) as http_mock:
    runner_wrapper = HttpLifecycleManager.wrap(runner_mock, mti, {'health': 31337})

    # ensure that start and stop are properly wrapped
    runner_wrapper.start(23.3)
    assert runner_mock.start.mock_calls == [mock.call(timeout=23.3)]

    # ensure that http teardown called when stopped
    runner_wrapper.stop(32.2)
    assert http_mock.mock_calls == [mock.call()]
    assert runner_mock.stop.mock_calls == [mock.call(timeout=32.2)]
