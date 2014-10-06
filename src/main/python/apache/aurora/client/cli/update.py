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

import json
import textwrap

from apache.aurora.client.cli import EXIT_API_ERROR, EXIT_OK, Noun, Verb
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    ALL_INSTANCES,
    BIND_OPTION,
    BROWSER_OPTION,
    CommandOption,
    CONFIG_ARGUMENT,
    HEALTHCHECK_OPTION,
    INSTANCES_SPEC_ARGUMENT,
    JOBSPEC_ARGUMENT,
    JOBSPEC_OPTION,
    JSON_READ_OPTION,
    JSON_WRITE_OPTION,
    ROLE_OPTION,
    STRICT_OPTION
)

from gen.apache.aurora.api.ttypes import JobUpdateAction, JobUpdateStatus


class StartUpdate(Verb):
  @property
  def name(self):
    return 'start'

  def get_options(self):
    return [
      BIND_OPTION, BROWSER_OPTION, JSON_READ_OPTION, HEALTHCHECK_OPTION, STRICT_OPTION,
      INSTANCES_SPEC_ARGUMENT, CONFIG_ARGUMENT
    ]

  @property
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

  @property
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

  @property
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

  @property
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


class ListUpdates(Verb):
  @property
  def name(self):
    return 'list'

  def get_options(self):
    return [
      JOBSPEC_OPTION,
      ROLE_OPTION,
      CommandOption("--user", default=None, metavar="username",
          help="The name of the user who initiated the update"),
      CommandOption("--status", choices=JobUpdateStatus._NAMES_TO_VALUES,
          default=None,
          action="append", help="Set of update statuses to search for"),
      JSON_WRITE_OPTION,
      CommandOption("cluster", metavar="clustername",
          help="Cluster to search for matching updates")]

  def help(self):
    return """List all jobs updates, with summary info, about active updates that match a query."""

  def execute(self, context):
    api = context.get_api(context.options.cluster)
    response = api.query_job_updates(
        role=context.options.role,
        jobKey=context.options.jobspec,
        user=context.options.user,
        update_statuses=context.options.status)
    context.check_and_log_response(response)
    if context.options.write_json:
      result = []
      for summary in response.result.getJobUpdateSummariesResult.updateSummaries:
        job_entry = {
            "jobkey": str(summary.jobKey),
            "id": summary.updateId,
            "user": summary.user,
            "started": summary.state.createdTimestampMs,
            "lastModified": summary.state.lastModifiedTimestampMs,
            "status": JobUpdateStatus._VALUES_TO_NAMES[summary.state.status]
        }
        result.append(job_entry)
      context.print_out(json.dumps(result, indent=2, separators=[',', ': '], sort_keys=False))
    else:
      for summary in response.result.getJobUpdateSummariesResult.updateSummaries:
        created = summary.state.createdTimestampMs
        lastMod = summary.state.lastModifiedTimestampMs
        context.print_out("Job: %s, Id: %s, User: %s, Status: %s" % (
            str(summary.jobKey),
            summary.updateId,
            summary.user,
            JobUpdateStatus._VALUES_TO_NAMES[summary.state.status]))
        context.print_out("Created: %s, Last Modified %s" % (created, lastMod), indent=2)
    return EXIT_OK


class UpdateStatus(Verb):
  @property
  def name(self):
    return 'status'

  def get_options(self):
    return [JSON_WRITE_OPTION, JOBSPEC_ARGUMENT]

  def help(self):
    return """Display detailed status information about an in-progress update."""

  def _get_update_id(self, context, jobkey):
    api = context.get_api(context.options.jobspec.cluster)
    response = api.query_job_updates(
        jobKey=context.options.jobspec)
    context.check_and_log_response(response, "")
    for summary in response.result.getJobUpdateSummariesResult.updateSummaries:
      if summary.jobKey == jobkey:
        return summary.updateId
    else:
      return None

  def execute(self, context):
    id = self._get_update_id(context, context.options.jobspec)
    api = context.get_api(context.options.jobspec.cluster)
    response = api.get_job_update_details(id)
    context.check_and_log_response(response)
    details = response.result.getJobUpdateDetailsResult.details
    if context.options.write_json:
      result = {}
      # the following looks odd, but it's needed to convince the json renderer
      # to render correctly.
      result["updateId"] = ("%s" % details.update.summary.updateId)
      result["job"] = str(context.options.jobspec)
      result["started"] = details.update.summary.state.createdTimestampMs
      result["last_updated"] = details.update.summary.state.lastModifiedTimestampMs
      result["status"] = JobUpdateStatus._VALUES_TO_NAMES[details.update.summary.state.status]
      result["update_events"] = []
      update_events = details.updateEvents
      if update_events is not None and len(update_events) > 0:
        for event in update_events:
          result["update_events"].append({
              "status": JobUpdateStatus._VALUES_TO_NAMES[event.status],
              "timestampMs": event.timestampMs})
      result["instance_update_events"] = []
      instance_events = details.instanceEvents
      if instance_events is not None and len(instance_events) > 0:
        for event in instance_events:
          result["instance_update_events"].append({
              "instance": event.instanceId,
              "timestamp": event.timestampMs,
              "action": JobUpdateAction._VALUES_TO_NAMES[event.action]
          })
      context.print_out(json.dumps(result, indent=2, separators=[',', ': '], sort_keys=False))

    else:
      context.print_out("Job: %s, UpdateID: %s" % (context.options.jobspec,
          details.update.summary.updateId))
      context.print_out("Started %s, last updated: %s" %
          (context.timestamp_to_string(details.update.summary.state.createdTimestampMs),
          context.timestamp_to_string(
              details.update.summary.state.lastModifiedTimestampMs)))
      context.print_out("Current status: %s" %
          JobUpdateStatus._VALUES_TO_NAMES[details.update.summary.state.status])
      update_events = details.updateEvents
      if update_events is not None and len(update_events) > 0:
        context.print_out("Update events:")
        for event in update_events:
          context.print_out("Status: %s at %s" % (
              JobUpdateStatus._VALUES_TO_NAMES[event.status],
              context.timestamp_to_string(event.timestampMs)
          ), indent=2)
      instance_events = details.instanceEvents
      if instance_events is not None and len(instance_events) > 0:
        context.print_out("Instance events:")
        for event in instance_events:
          context.print_out("Instance %s at %s: %s" % (
            event.instanceId, context.timestamp_to_string(event.timestampMs),
            JobUpdateAction._VALUES_TO_NAMES[event.action]
          ), indent=2)
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
    self.register_verb(ListUpdates())
    self.register_verb(UpdateStatus())
