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

import datetime
import json
import textwrap
import time
from collections import namedtuple

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
    JSON_READ_OPTION,
    JSON_WRITE_OPTION,
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
        Start a rolling update of a running job, using the update configuration within the config
        file as a control for update velocity and failure tolerance.

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
    return """Pause an update."""

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
    return """Resume an update."""

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
    return """Abort an in-progress update."""

  def execute(self, context):
    job_key = context.options.jobspec
    return UpdateController(context.get_api(job_key.cluster), context).abort(
        job_key,
        context.options.message)


UpdateFilter = namedtuple('UpdateFilter', ['cluster', 'role', 'env', 'job'])


class ListUpdates(Verb):
  @staticmethod
  def update_filter(filter_str):
    if filter_str is None or filter_str == '':
      raise ValueError('Update filter must be non-empty')
    parts = filter_str.split('/')
    if len(parts) == 0 or len(parts) > 4:
      raise ValueError('Update filter must be a path of the form CLUSTER/ROLE/ENV/JOB.')
    # Pad with None.
    parts = parts + ([None] * (4 - len(parts)))
    return UpdateFilter(
        cluster=parts[0],
        role=parts[1],
        env=parts[2],
        job=parts[3])

  @property
  def name(self):
    return 'list'

  STATUS_GROUPS = dict({
      'active': ACTIVE_JOB_UPDATE_STATES,
      'all': JobUpdateStatus._VALUES_TO_NAMES.keys(),
      'blocked': [
          JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE, JobUpdateStatus.ROLL_BACK_AWAITING_PULSE],
      'failed': [JobUpdateStatus.ERROR, JobUpdateStatus.FAILED, JobUpdateStatus.ROLLED_BACK],
      'inactive': list(set(JobUpdateStatus._VALUES_TO_NAMES.keys()) - ACTIVE_JOB_UPDATE_STATES),
      'paused': [JobUpdateStatus.ROLL_FORWARD_PAUSED, JobUpdateStatus.ROLL_BACK_PAUSED],
      'succeeded': JobUpdateStatus.ROLLED_FORWARD,
  }.items() + JobUpdateStatus._NAMES_TO_VALUES.items())

  def get_options(self):
    return [
      CommandOption(
          'filter',
          type=self.update_filter,
          metavar="CLUSTER[/ROLE[/ENV[/JOB]]]",
          help=('A path-like specifier for the scope of updates to list.')),
      CommandOption(
          "--status",
          choices=self.STATUS_GROUPS,
          default=[],
          action="append",
          help="""Update state to filter by. This may be specified multiple times, in which case
updates matching any of the specified statuses will be included."""),
      CommandOption("--user", default=None, metavar="username",
          help="The name of the user who initiated the update"),
      JSON_WRITE_OPTION
    ]

  @property
  def help(self):
    return "List summaries of job updates."

  COLUMNS = [
    ('JOB', 47),
    ('UPDATE ID', 36),
    ('STATUS', 15),
    ('CREATED BY', 11),
    ('STARTED', 19),
    ('LAST MODIFIED', 19)
  ]
  FORMAT_STR = ' '.join(["{%d:%d}" % (i, col[1]) for i, col in enumerate(COLUMNS)])
  HEADER = FORMAT_STR.format(*[c[0] for c in COLUMNS])

  def execute(self, context):
    update_filter = context.options.filter
    cluster = update_filter.cluster
    if (update_filter.role is not None
        and update_filter.env is not None
        and update_filter.job is not None):

      job_key = AuroraJobKey(
          cluster=cluster,
          role=update_filter.role,
          env=update_filter.env,
          name=update_filter.job)
    else:
      job_key = None

    api = context.get_api(cluster)

    filter_statuses = set()
    for status in context.options.status:
      group = self.STATUS_GROUPS[status]
      if isinstance(group, list):
        filter_statuses = filter_statuses.union(set(group))
      else:
        filter_statuses.add(group)

    response = api.query_job_updates(
        role=update_filter.role if job_key is None else None,
        job_key=job_key,
        update_statuses=filter_statuses if filter_statuses else None,
        user=context.options.user)
    context.log_response_and_raise(response)

    # The API does not offer a way to query by environment, so if that filter is requested, we
    # perform a more broad role-based query and filter here.
    summaries = response.result.getJobUpdateSummariesResult.updateSummaries
    if job_key is None and update_filter.env is not None:
      summaries = [s for s in summaries if s.key.job.environment == update_filter.env]

    def format_timestamp(stamp_millis):
      return datetime.datetime.utcfromtimestamp(stamp_millis / 1000).isoformat()

    if context.options.write_json:
      result = []
      for summary in summaries:
        job_entry = {
            "job": AuroraJobKey.from_thrift(cluster, summary.key.job).to_path(),
            "id": summary.key.id,
            "user": summary.user,
            "started": format_timestamp(summary.state.createdTimestampMs),
            "lastModified": format_timestamp(summary.state.lastModifiedTimestampMs),
            "status": JobUpdateStatus._VALUES_TO_NAMES[summary.state.status]
        }
        result.append(job_entry)
      context.print_out(json.dumps(result, indent=2, separators=[',', ': '], sort_keys=False))
    else:
      if summaries:
        context.print_out(self.HEADER)
      for summary in summaries:
        context.print_out(self.FORMAT_STR.format(
            AuroraJobKey.from_thrift(cluster, summary.key.job).to_path(),
            summary.key.id,
            JobUpdateStatus._VALUES_TO_NAMES[summary.state.status],
            summary.user,
            format_timestamp(summary.state.createdTimestampMs),
            format_timestamp(summary.state.lastModifiedTimestampMs))
        )
    return EXIT_OK


class UpdateStatus(Verb):
  @property
  def name(self):
    return 'status'

  def get_options(self):
    return [JSON_WRITE_OPTION, JOBSPEC_ARGUMENT]

  @property
  def help(self):
    return """Display detailed status information about an in-progress update."""

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
    return "update"

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
