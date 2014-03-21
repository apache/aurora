#
# Copyright 2014 Apache Software Foundation
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

from apache.aurora.client.cli import (
    EXIT_OK,
    Noun,
    Verb,
)
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    CommandOption,
    JOBSPEC_ARGUMENT,
    parse_percentiles,
    parse_time_values
)

from twitter.common.quantity import Time


class GetTaskUpCountCmd(Verb):
  @property
  def name(self):
    return 'get-task-up-count'

  @property
  def help(self):
    return """Print the percentage of tasks that stayed up within the last "duration" s|m|h|d.
If duration is not specified prints a histogram-like log-scale distribution
of task uptime percentages.
"""

  @classmethod
  def render_get_task_up_count(cls, context, vector):
    def format_output(durations):
      return ['%s\t- %.2f %%' % (duration, vector.get_task_up_count(duration.as_(Time.SECONDS)))
          for duration in durations]

    durations = context.options.durations or parse_time_values('1m,10m,1h,12h,7d')
    return '\n'.join(format_output(durations))

  def get_options(self):
    return [
        CommandOption('--durations', type=parse_time_values, default=None,
            help="""Durations to report uptime for.
Format: XdYhZmWs (each field optional but must be in that order.)
Examples:
  --durations=1d'
  --durations=3m,10s,1h3m10s"""),
        JOBSPEC_ARGUMENT]

  def execute(self, context):
    api = context.get_api(context.options.jobspec.cluster)
    vector = api.sla_get_job_uptime_vector(context.options.jobspec)
    context.print_out(self.render_get_task_up_count(context, vector))
    return EXIT_OK


class GetJobUptimeCmd(Verb):
  @property
  def name(self):
    return 'get-job-uptime'

  @property
  def help(self):
    return """Prints the job uptime at the specified percentile. If the percentile
is not specified prints a histogram-like distribution of uptime percentiles.
"""

  @classmethod
  def render_get_job_uptime(cls, context, vector):
    def format_output(percentiles):
      return ['%s percentile\t- %s seconds' % (percentile, vector.get_job_uptime(percentile))
              for percentile in sorted(percentiles, reverse=True)]

    percentiles = context.options.percentiles or parse_percentiles('99,95,90,85,75,60,50,30,10')
    return '\n'.join(format_output(percentiles))

  def get_options(self):
    return [
        CommandOption('--percentiles', type=parse_percentiles, default=None,
            help="""Percentiles to report uptime for. Format: values within (0.0, 100.0).
Example: --percentiles=50,75,95.5"""),
        JOBSPEC_ARGUMENT]

  def execute(self, context):
    api = context.get_api(context.options.jobspec.cluster)
    vector = api.sla_get_job_uptime_vector(context.options.jobspec)
    context.print_out(self.render_get_job_uptime(context, vector))
    return EXIT_OK


class Sla(Noun):
  @property
  def name(self):
    return 'sla'

  @property
  def help(self):
    return 'Work with SLA data in Aurora cluster.'

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(Sla, self).__init__()
    self.register_verb(GetTaskUpCountCmd())
    self.register_verb(GetJobUptimeCmd())
