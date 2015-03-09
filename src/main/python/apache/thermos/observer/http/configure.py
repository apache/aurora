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

from twitter.common.http import HttpServer
from twitter.common.http.diagnostics import DiagnosticsEndpoints
from twitter.common.metrics import RootMetrics

from .diagnostics import register_build_properties, register_diagnostics
from .http_observer import BottleObserver
from .vars_endpoint import VarsEndpoint


def configure_server(task_observer):
  bottle_wrapper = BottleObserver(task_observer)
  root_metrics = RootMetrics()
  server = HttpServer()
  server.mount_routes(bottle_wrapper)
  server.mount_routes(DiagnosticsEndpoints())
  server.mount_routes(VarsEndpoint())
  register_build_properties(root_metrics)
  register_diagnostics(root_metrics)
  return server
