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
import time

from apache.aurora.client.base import combine_messages
from apache.aurora.client.cli import (
    EXIT_API_ERROR,
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_PARAMETER,
    EXIT_OK,
    Noun,
    Verb
)
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
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.api.constants import ACTIVE_JOB_UPDATE_STATES
from gen.apache.aurora.api.ttypes import JobUpdateAction, JobUpdateStatus


class UpdateController(object):
  def __init__(self, api, context):
    self.api = api
    self.context = context

  def get_update_key(self, job_key):
    response = self.api.query_job_updates(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=job_key)
    self.context.log_response_and_raise(response)
    summaries = response.result.getJobUpdateSummariesResult.updateSummaries
    if response.result.getJobUpdateSummariesResult.updateSummaries:
      if len(summaries) == 1:
        return summaries[0].key
      else:
        raise self.context.CommandError(
            EXIT_API_ERROR,
            "scheduler returned multiple active updates for this job.")
    else:
      return None

  def _modify_update(self, job_key, mutate_fn, error_msg, success_msg):
    update_key = self.get_update_key(job_key)
    if update_key is None:
      self.context.print_err("No active update found for this job.")
      return EXIT_INVALID_PARAMETER
    resp = mutate_fn(update_key)
    self.context.log_response_and_raise(resp, err_code=EXIT_API_ERROR, err_msg=error_msg)
    self.context.print_out(success_msg)
    return EXIT_OK

  def pause(self, job_key, message):
    return self._modify_update(
        job_key,
        lambda key: self.api.pause_job_update(key, message),
        "Failed to pause update due to error:",
        "Update has been paused.")

  def resume(self, job_key, message):
    return self._modify_update(
        job_key,
        lambda key: self.api.resume_job_update(key, message),
        "Failed to resume update due to error:",
        "Update has been resumed.")

  def abort(self, job_key, message):
    return self._modify_update(
        job_key,
        lambda key: self.api.abort_job_update(key, message),
        "Failed to abort update due to error:",
        "Update has been aborted.")


MESSAGE_OPTION = CommandOption(
    '--message',
    '-m',
    type=str,
    default=None,
    help='Message to include with the update state transition')


class StartUpdate(Verb):

  UPDATE_MSG_TEMPLATE = "Job update has started. View your update progress at %s"

  @property
  def name(self):
    return 'start'

  def get_options(self):
    return [
        BIND_OPTION,
        BROWSER_OPTION,
        HEALTHCHECK_OPTION,
        JSON_READ_OPTION,
        MESSAGE_OPTION,
        STRICT_OPTION,
        INSTANCES_SPEC_ARGUMENT,
        CONFIG_ARGUMENT
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

  def execute(self, context):
    job = context.options.instance_spec.jobkey
    instances = (None if context.options.instance_spec.instance == ALL_INSTANCES else
        context.options.instance_spec.instance)
    config = context.get_job_config(job, context.options.config_file)
    if config.raw().has_cron_schedule():
      raise context.CommandError(
          EXIT_COMMAND_FAILURE,
          "Cron jobs may only be updated with \"aurora cron schedule\" command")

    api = context.get_api(config.cluster())
    resp = api.start_job_update(config, context.options.message, instances)
    context.log_response_and_raise(resp, err_code=EXIT_API_ERROR,
        err_msg="Failed to start update due to error:")

    if resp.result:
      url = context.get_update_page(api, config.cluster(), resp.result.startJobUpdateResult.key)
      context.print_out(self.UPDATE_MSG_TEMPLATE % url)
    else:
      context.print_out(combine_messages(resp))
    return EXIT_OK


class PauseUpdate(Verb):
  @property
  def name(self):
    return 'pause'

  def get_options(self):
    return [JOBSPEC_ARGUMENT, MESSAGE_OPTION]

  @property
  def help(self):
    return """Pause a scheduler-driven rolling update."""

  def execute(self, context):
    job_key = context.options.jobspec
    return UpdateController(context.get_api(job_key.cluster), context).pause(
        job_key,
        context.options.message)


class ResumeUpdate(Verb):
  @property
  def name(self):
    return 'resume'

  def get_options(self):
    return [JOBSPEC_ARGUMENT, MESSAGE_OPTION]

  @property
  def help(self):
    return """Resume a paused scheduler-driven rolling update."""

  def execute(self, context):
    job_key = context.options.jobspec
    return UpdateController(context.get_api(job_key.cluster), context).resume(
        job_key,
        context.options.message)


class AbortUpdate(Verb):
  @property
  def name(self):
    return 'abort'

  def get_options(self):
    return [JOBSPEC_ARGUMENT, MESSAGE_OPTION]

  @property
  def help(self):
    return """Abort an in-progress scheduler-driven rolling update."""

  def execute(self, context):
    job_key = context.options.jobspec
    return UpdateController(context.get_api(job_key.cluster), context).abort(
        job_key,
        context.options.message)


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

  @property
  def help(self):
    return textwrap.dedent("""\
        List all scheduler-driven jobs updates, with summary info, about active updates
        that match a query.
        """)

  def execute(self, context):
    cluster = context.options.cluster
    api = context.get_api(cluster)
    response = api.query_job_updates(
        role=context.options.role,
        job_key=context.options.jobspec,
        user=context.options.user,
        update_statuses=context.options.status)
    context.log_response_and_raise(response)
    if context.options.write_json:
      result = []
      for summary in response.result.getJobUpdateSummariesResult.updateSummaries:
        job_entry = {
            "jobkey": AuroraJobKey.from_thrift(cluster, summary.key.job).to_path(),
            "id": summary.key.id,
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
            AuroraJobKey.from_thrift(cluster, summary.key.job).to_path(),
            summary.key.id,
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

  @property
  def help(self):
    return """Display detailed status information about a scheduler-driven in-progress update."""

  def execute(self, context):
    key = UpdateController(
        context.get_api(context.options.jobspec.cluster),
        context).get_update_key(context.options.jobspec)
    if key is None:
      context.print_err("No updates found for job %s" % context.options.jobspec)
      return EXIT_INVALID_PARAMETER

    api = context.get_api(context.options.jobspec.cluster)
    response = api.get_job_update_details(key)
    context.log_response_and_raise(response)
    details = response.result.getJobUpdateDetailsResult.details
    if context.options.write_json:
      result = {
        "updateId": ("%s" % details.update.summary.key.id),
        "job": str(context.options.jobspec),
        "started": details.update.summary.state.createdTimestampMs,
        "last_updated": details.update.summary.state.lastModifiedTimestampMs,
        "status": JobUpdateStatus._VALUES_TO_NAMES[details.update.summary.state.status],
        "update_events": [],
        "instance_update_events": []
      }

      update_events = details.updateEvents
      if update_events is not None and len(update_events) > 0:
        for event in update_events:
          event_data = {
              "status": JobUpdateStatus._VALUES_TO_NAMES[event.status],
              "timestampMs": event.timestampMs
          }
          if event.message:
            event_data["message"] = event.message
          result["update_events"].append(event_data)

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
      def timestamp(time_ms):
        return time.ctime(time_ms / 1000)

      context.print_out("Job: %s, UpdateID: %s" % (context.options.jobspec,
          details.update.summary.key.id))
      context.print_out("Started %s, last activity: %s" %
          (timestamp(details.update.summary.state.createdTimestampMs),
          timestamp(details.update.summary.state.lastModifiedTimestampMs)))
      context.print_out("Current status: %s" %
          JobUpdateStatus._VALUES_TO_NAMES[details.update.summary.state.status])
      update_events = details.updateEvents
      if update_events is not None and len(update_events) > 0:
        context.print_out("Update events:")
        for event in update_events:
          context.print_out("Status: %s at %s" % (
              JobUpdateStatus._VALUES_TO_NAMES[event.status],
              timestamp(event.timestampMs)
          ), indent=2)
          if event.message:
            context.print_out("  message: %s" % event.message, indent=4)
      instance_events = details.instanceEvents
      if instance_events is not None and len(instance_events) > 0:
        context.print_out("Instance events:")
        for event in instance_events:
          context.print_out("Instance %s at %s: %s" % (
            event.instanceId, timestamp(event.timestampMs),
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
