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

from __future__ import print_function

import textwrap

from apache.aurora.client.cli import (
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_PARAMETER,
    EXIT_OK,
    Noun,
    Verb
)
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    BIND_OPTION,
    BROWSER_OPTION,
    CONFIG_ARGUMENT,
    CONFIG_OPTION,
    JOBSPEC_ARGUMENT,
    JSON_READ_OPTION
)


class Schedule(Verb):
  @property
  def name(self):
    return "schedule"

  @property
  def help(self):
    return textwrap.dedent("""\
       Create a cron schedule for a job or replace the existing cron template with a new one.
       Only future runs will be affected, any existing active tasks are left intact.""")

  def get_options(self):
    return [BIND_OPTION, JSON_READ_OPTION, JOBSPEC_ARGUMENT, CONFIG_ARGUMENT]

  def execute(self, context):
    api = context.get_api(context.options.jobspec.cluster)
    config = context.get_job_config(context.options.jobspec, context.options.config_file)
    if not config.raw().has_cron_schedule():
      raise context.CommandError(
          EXIT_COMMAND_FAILURE,
          "Non-cron jobs may only be created with \"aurora job create\" command")

    resp = api.schedule_cron(config)
    context.log_response_and_raise(resp,
        err_msg=("Error scheduling cron job %s:" % context.options.jobspec))

    context.print_out("Cron job scheduled, status can be viewed at %s"
        % context.get_job_page(api, context.options.jobspec))

    return EXIT_OK


class Deschedule(Verb):
  @property
  def name(self):
    return "deschedule"

  @property
  def help(self):
    return textwrap.dedent("""\
        Remove the cron schedule for a job. Any active tasks are not affected.
        Use \"aurora job kill\" command to terminate active tasks.""")

  def get_options(self):
    return [JOBSPEC_ARGUMENT]

  def execute(self, context):
    api = context.get_api(context.options.jobspec.cluster)
    resp = api.deschedule_cron(context.options.jobspec)
    context.log_response_and_raise(resp,
        err_msg=("Error descheduling cron job %s:" % context.options.jobspec))
    context.print_out("Cron descheduling succeeded.")
    return EXIT_OK


class Start(Verb):
  @property
  def name(self):
    return "start"

  @property
  def help(self):
    return """Start a cron job immediately, outside of its normal cron schedule."""

  def get_options(self):
    return [BIND_OPTION, BROWSER_OPTION, CONFIG_OPTION, JSON_READ_OPTION, JOBSPEC_ARGUMENT]

  def execute(self, context):
    api = context.get_api(context.options.jobspec.cluster)
    config = (context.get_job_config(context.options.jobspec, context.options.config)
        if context.options.config else None)
    resp = api.start_cronjob(context.options.jobspec, config=config)
    context.log_response_and_raise(resp,
        err_msg=("Error starting cron job %s:" % context.options.jobspec))
    if context.options.open_browser:
      context.open_job_page(api, context.options.jobspec)
    return EXIT_OK


class Show(Verb):
  @property
  def name(self):
    return "show"

  @property
  def help(self):
    return """Get the scheduling status of a cron job"""

  def get_options(self):
    return [JOBSPEC_ARGUMENT]

  def execute(self, context):
    #TODO(mchucarroll): do we want to support wildcards here?
    jobkey = context.options.jobspec
    api = context.get_api(jobkey.cluster)
    resp = api.get_jobs(jobkey.role)
    context.log_response_and_raise(resp, err_code=EXIT_INVALID_PARAMETER,
        err_msg=("Error getting cron status for %self from server" % jobkey))
    for job in resp.result.getJobsResult.configs:
      if job.key.environment == jobkey.env and job.key.name == jobkey.name:
        if job.cronSchedule is None or job.cronSchedule == "":
          context.print_err("No cron entry found for job %s" % jobkey)
          return EXIT_INVALID_PARAMETER
        else:
          context.print_out("%s\t %s" % (jobkey, job.cronSchedule))
          return EXIT_OK
    context.print_err("No cron entry found for job %s" % jobkey)
    return EXIT_INVALID_PARAMETER


class CronNoun(Noun):
  @property
  def name(self):
    return "cron"

  @property
  def help(self):
    return "Work with entries in the aurora cron scheduler"

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(CronNoun, self).__init__()
    self.register_verb(Schedule())
    self.register_verb(Deschedule())
    self.register_verb(Start())
    self.register_verb(Show())
