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

from apache.aurora.client.cli import EXIT_API_ERROR, EXIT_OK, Noun, Verb
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    ALL_INSTANCES,
    BIND_OPTION,
    BROWSER_OPTION,
    CONFIG_ARGUMENT,
    HEALTHCHECK_OPTION,
    INSTANCES_SPEC_ARGUMENT,
    JOBSPEC_ARGUMENT,
    JSON_READ_OPTION,
    STRICT_OPTION
)


class StartUpdate(Verb):
  @property
  def name(self):
    return 'start'

  def get_options(self):
    return [
      BIND_OPTION, BROWSER_OPTION, JSON_READ_OPTION, HEALTHCHECK_OPTION, STRICT_OPTION,
      INSTANCES_SPEC_ARGUMENT, CONFIG_ARGUMENT
    ]

  def help(self):
    return textwrap.dedent("""\
        Start a scheduler-driven rolling upgrade on a running job, using the update
        configuration within the config file as a control for update velocity and failure
        tolerance.

        The updater only takes action on instances in a job that have changed, meaning
        that changing a single instance will only induce a restart on the changed task instance.

        You may want to consider using the 'aurora job diff' subcommand before updating,
        to preview what changes will take effect.
        """)
  # TODO(mchucarroll): consider adding an "aurora update preview"?

  def execute(self, context):
    job = context.options.instance_spec.jobkey
    instances = (None if context.options.instance_spec.instance == ALL_INSTANCES else
        context.options.instance_spec.instance)
    if instances is not None and context.options.strict:
      context.verify_shards_option_validity(job, instances)
    config = context.get_job_config(job, context.options.config_file)
    api = context.get_api(config.cluster())
    resp = api.start_job_update(config, instances)
    context.check_and_log_response(resp, err_code=EXIT_API_ERROR,
        err_msg="Failed to start scheduler-driven update; see log for details.")
    context.print_out("Scheduler-driven update of job %s has started." % job)
    return EXIT_OK


class PauseUpdate(Verb):
  @property
  def name(self):
    return 'pause'

  def get_options(self):
    return [
      JOBSPEC_ARGUMENT
    ]

  def help(self):
    return """Pause a scheduler-driven rolling update."""

  def execute(self, context):
    jobkey = context.options.jobspec
    api = context.get_api(jobkey.cluster)
    resp = api.pause_job_update(jobkey)
    context.check_and_log_response(resp, err_code=EXIT_API_ERROR,
      err_msg="Failed to pause scheduler-driven update; see log for details")
    context.print_out("Scheduler-driven update of job %s has been paused." % jobkey)
    return EXIT_OK


class ResumeUpdate(Verb):
  @property
  def name(self):
    return 'resume'

  def get_options(self):
    return [
      JOBSPEC_ARGUMENT
    ]

  def help(self):
    return """Resume a paused scheduler-driven rolling update."""

  def execute(self, context):
    jobkey = context.options.jobspec
    api = context.get_api(jobkey.cluster)
    resp = api.resume_job_update(jobkey)
    context.check_and_log_response(resp, err_code=EXIT_API_ERROR,
      err_msg="Failed to resume scheduler-driven update; see log for details")
    context.print_out("Scheduler-driven update of job %s has been resumed." % jobkey)
    return EXIT_OK


class AbortUpdate(Verb):
  @property
  def name(self):
    return 'abort'

  def get_options(self):
    return [
      JOBSPEC_ARGUMENT
    ]

  def help(self):
    return """Abort an in-pregress scheduler-driven rolling update."""

  def execute(self, context):
    jobkey = context.options.jobspec
    api = context.get_api(jobkey.cluster)
    resp = api.abort_job_update(jobkey)
    context.check_and_log_response(resp, err_code=EXIT_API_ERROR,
      err_msg="Failed to abort scheduler-driven update; see log for details")
    context.print_out("Scheduler-driven update of job %s has been aborted." % jobkey)
    return EXIT_OK


class Update(Noun):
  @property
  def name(self):
    return "beta-update"

  @property
  def help(self):
    return "Interact with the aurora update service."

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(Update, self).__init__()
    self.register_verb(StartUpdate())
    self.register_verb(PauseUpdate())
    self.register_verb(ResumeUpdate())
    self.register_verb(AbortUpdate())
