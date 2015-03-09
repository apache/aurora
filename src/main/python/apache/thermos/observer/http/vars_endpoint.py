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
from twitter.common.http.server import request
from twitter.common.metrics import MetricSampler, RootMetrics


class VarsEndpoint(object):
  """
    Wrap a MetricSampler to export the /vars endpoint for applications that register
    exported variables.
  """

  def __init__(self, period=None, stats_filter=None):
    self._metrics = RootMetrics()
    self._stats_filter = stats_filter
    if period is not None:
      self._monitor = MetricSampler(self._metrics, period)
    else:
      self._monitor = MetricSampler(self._metrics)
    self._monitor.start()

  @HttpServer.route("/vars")
  @HttpServer.route("/vars/:var")
  def handle_vars(self, var=None):
    HttpServer.set_content_type('text/plain; charset=iso-8859-1')
    filtered = self._parse_filtered_arg()
    samples = self._monitor.sample()

    if var is None and filtered and self._stats_filter:
      return '\n'.join(
          '%s %s' % (key, val) for key, val in sorted(samples.items())
          if not self._stats_filter.match(key))
    elif var is None:
      return '\n'.join(
          '%s %s' % (key, val) for key, val in sorted(samples.items()))
    else:
      if var in samples:
        return samples[var]
      else:
        HttpServer.abort(404, 'Unknown exported variable')

  @HttpServer.route("/vars.json")
  def handle_vars_json(self, var=None, value=None):
    filtered = self._parse_filtered_arg()
    sample = self._monitor.sample()
    if filtered and self._stats_filter:
      return dict((key, val) for key, val in sample.items() if not self._stats_filter.match(key))
    else:
      return sample

  def shutdown(self):
    self._monitor.shutdown()
    self._monitor.join()

  def _parse_filtered_arg(self):
    return request.GET.get('filtered', '') in ('true', '1')
