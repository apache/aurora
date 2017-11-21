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
import logging
import pprint
import textwrap
import webbrowser
from collections import namedtuple
from copy import deepcopy
from datetime import datetime

from thrift.protocol import TJSONProtocol
from thrift.TSerialization import serialize

from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.api.restarter import RestartSettings
from apache.aurora.client.base import get_job_page, synthesize_url
from apache.aurora.client.cli import (
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_CONFIGURATION,
    EXIT_INVALID_PARAMETER,
    EXIT_OK,
    EXIT_TIMEOUT,
    Noun,
    Verb
)
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.diff_formatter import DiffFormatter
from apache.aurora.client.cli.options import (
    ADD_INSTANCE_WAIT_OPTION,
    ALL_INSTANCES,
    BATCH_OPTION,
    BIND_OPTION,
    BROWSER_OPTION,
    CONFIG_ARGUMENT,
    CONFIG_OPTION,
    FORCE_OPTION,
    HEALTHCHECK_OPTION,
    INSTANCES_SPEC_ARGUMENT,
    JOBSPEC_ARGUMENT,
    JSON_READ_OPTION,
    JSON_WRITE_OPTION,
    MAX_TOTAL_FAILURES_OPTION,
    NO_BATCHING_OPTION,
    STRICT_OPTION,
    TASK_INSTANCE_ARGUMENT,
    WATCH_OPTION,
    CommandOption
)
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.config.resource import ResourceManager

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import ResponseCode, ScheduleStatus

# Utility type, representing job keys with wildcards.
PartialJobKey = namedtuple('PartialJobKey', ['cluster', 'role', 'env', 'name'])


def arg_type_jobkey(key):
  """Given a partial jobkey, where parts can be wildcards, parse it.
  Slots that are wildcards will be replaced by "*".
  """
  parts = []
  for part in key.split('/'):
    parts.append(part)
  if len(parts) > 4:
    raise ValueError('Job key must have no more than 4 segments')
  while len(parts) < 4:
    parts.append('*')
  return PartialJobKey(*parts)

WILDCARD_JOBKEY_OPTION = CommandOption("jobspec", type=arg_type_jobkey,
        metavar="cluster[/role[/env[/name]]]",
        help="A jobkey, optionally containing wildcards")


def wait_until(wait_until_option, job_key, api, instances=None):
  """Waits for job instances to reach specified status.

  :param wait_until_option: Expected instance status
  :type wait_until_option: ADD_INSTANCE_WAIT_OPTION
  :param job_key: Job key to wait on
  :type job_key: AuroraJobKey
  :param api: Aurora scheduler API
  :type api: AuroraClientAPI
  :param instances: Specific instances to wait on
  :type instances: set of int
  """
  if wait_until_option == "RUNNING":
    JobMonitor(api.scheduler_proxy, job_key).wait_until(
        JobMonitor.running_or_finished,
        instances=instances)
  elif wait_until_option == "FINISHED":
    JobMonitor(api.scheduler_proxy, job_key).wait_until(JobMonitor.terminal, instances=instances)


class CreateJobCommand(Verb):
  @property
  def name(self):
    return "create"

  @property
  def help(self):
    return "Create a service or ad hoc job using aurora"

  def get_options(self):
    return [BIND_OPTION, JSON_READ_OPTION,
        ADD_INSTANCE_WAIT_OPTION,
        BROWSER_OPTION,
        JOBSPEC_ARGUMENT, CONFIG_ARGUMENT]

  def execute(self, context):
    config = context.get_job_config(context.options.jobspec, context.options.config_file)
    if config.raw().has_cron_schedule():
      raise context.CommandError(
          EXIT_COMMAND_FAILURE,
          "Cron jobs may only be scheduled with \"aurora cron schedule\" command")

    api = context.get_api(config.cluster())
    resp = api.create_job(config)
    context.log_response_and_raise(resp, err_code=EXIT_COMMAND_FAILURE,
                                   err_msg="Job creation failed due to error:")
    if context.options.open_browser:
      webbrowser.open_new_tab(get_job_page(api, context.options.jobspec))

    wait_until(context.options.wait_until, config.job_key(), api)

    # Check to make sure the job was created successfully.
    status_response = api.check_status(config.job_key())
    if (status_response.responseCode is not ResponseCode.OK or
        status_response.result.scheduleStatusResult.tasks is None or
        status_response.result.scheduleStatusResult.tasks == []):
      context.print_err("Error occurred while creating job %s" % context.options.jobspec)
      return EXIT_COMMAND_FAILURE
    else:
      context.print_out("Job create succeeded: job url=%s" %
                        get_job_page(api, context.options.jobspec))
    return EXIT_OK


class DiffCommand(Verb):
  def __init__(self):
    super(DiffCommand, self).__init__()
    self.prettyprinter = pprint.PrettyPrinter(indent=2)

  @property
  def help(self):
    return textwrap.dedent("""\
        Compare a job configuration against a running job.
        By default the diff will be displayed using 'diff', though you may choose an
        alternate diff command by setting the DIFF_VIEWER environment variable.""")

  @property
  def name(self):
    return "diff"

  def get_options(self):
    return [
        BIND_OPTION,
        JSON_READ_OPTION,
        CommandOption("--from", dest="rename_from", type=AuroraJobKey.from_path, default=None,
            metavar="cluster/role/env/name",
            help="If specified, the job key to diff against."),
        INSTANCES_SPEC_ARGUMENT,
        CONFIG_ARGUMENT]

  def execute(self, context):
    config = context.get_job_config(
        context.options.instance_spec.jobkey,
        context.options.config_file)
    if context.options.rename_from is not None:
      cluster = context.options.rename_from.cluster
      role = context.options.rename_from.role
      env = context.options.rename_from.environment
      name = context.options.rename_from.name
    else:
      cluster = config.cluster()
      role = config.role()
      env = config.environment()
      name = config.name()
    api = context.get_api(cluster)

    resp = api.populate_job_config(config)
    context.log_response_and_raise(
        resp,
        err_code=EXIT_INVALID_CONFIGURATION,
        err_msg="Error loading configuration")
    local_task = resp.result.populateJobResult.taskConfig
    # Deepcopy is important here as tasks will be modified for printing.
    local_tasks = [
        deepcopy(local_task) for _ in range(config.instances())
    ]
    instances = (None if context.options.instance_spec.instance == ALL_INSTANCES else
                 context.options.instance_spec.instance)
    formatter = DiffFormatter(context, config, cluster, role, env, name)

    if config.raw().has_cron_schedule():
      formatter.diff_no_update_details(local_tasks)
    else:
      formatter.show_job_update_diff(instances, local_task)

    return EXIT_OK


class InspectCommand(Verb):

  @property
  def help(self):
    return textwrap.dedent("""\
        Verify that a job can be parsed from a configuration file, and display
        the parsed configuration.""")

  @property
  def name(self):
    return "inspect"

  def get_options(self):
    return [BIND_OPTION, JSON_READ_OPTION, JSON_WRITE_OPTION,
        CommandOption("--raw", dest="raw", default=False, action="store_true",
            help="Show the raw configuration."),
        JOBSPEC_ARGUMENT, CONFIG_ARGUMENT]

  def _render_config_pretty(self, config, context):
    """Render the config description in human-friendly format"""
    job = config.raw()
    job_thrift = config.job()
    context.print_out("Job level information")
    context.print_out("name:       '%s'" % job.name(), indent=2)
    context.print_out("role:       '%s'" % job.role(), indent=2)
    if job.has_contact():
      context.print_out("contact:    '%s'" % job.contact(), indent=2)
    context.print_out("cluster:    '%s'" % job.cluster(), indent=2)
    context.print_out("instances:  '%s'" % job.instances(), indent=2)
    if job.has_cron_schedule():
      context.print_out("cron:", indent=2)
      context.print_out("schedule: '%s'" % job.cron_schedule(), indent=4)
      context.print_out("policy:   '%s'" % job.cron_collision_policy(), indent=4)
    if job.has_constraints():
      context.print_out("constraints:", indent=2)
      for constraint, value in job.constraints().get().items():
        context.print_out("'%s': '%s'" % (constraint, value), indent=4)
    if job.has_partition_policy():
      context.print_out("partition_policy:", indent=2)
      context.print_out("reschedule: %s" % job.partition_policy().reschedule(), indent=4)
      context.print_out("delay_secs: '%s'" % job.partition_policy().delay_secs(), indent=4)

    context.print_out("service:    %s" % job_thrift.taskConfig.isService, indent=2)
    context.print_out("production: %s" % bool(job.production().get()), indent=2)
    context.print_out("")

    task = job.task()
    context.print_out("Task level information")
    context.print_out("name: '%s'" % task.name(), indent=2)

    if len(task.constraints().get()) > 0:
      context.print_out("constraints:", indent=2)
      for constraint in task.constraints():
        context.print_out("%s" % (" < ".join(st.get() for st in constraint.order() or [])),
            indent=2)
    context.print_out("")

    processes = task.processes()
    for process in processes:
      context.print_out("Process '%s':" % process.name())
      if process.daemon().get():
        context.print_out("daemon", indent=2)
      if process.ephemeral().get():
        context.print_out("ephemeral", indent=2)
      if process.final().get():
        context.print_out("final", indent=2)
      context.print_out("cmdline:", indent=2)
      for line in process.cmdline().get().splitlines():
        context.print_out(line, indent=4)
      context.print_out("")
    return EXIT_OK

  def execute(self, context):
    config = context.get_job_config(context.options.jobspec, context.options.config_file)
    if context.options.raw:
      context.print_out(str(config.job()))
      return EXIT_OK

    if context.options.write_json:
      context.print_out(config.raw().json_dumps())
      return EXIT_OK
    else:
      return self._render_config_pretty(config, context)


class AbstractKillCommand(Verb):
  def get_options(self):
    return [BROWSER_OPTION,
        BIND_OPTION,
        JSON_READ_OPTION,
        CONFIG_OPTION,
        BATCH_OPTION,
        MAX_TOTAL_FAILURES_OPTION,
        NO_BATCHING_OPTION,
        CommandOption('--message', '-m', type=str, default=None,
                      help='Message to include with the kill state transition')]

  def wait_kill_tasks(self, context, scheduler, job_key, instances=None):
    monitor = JobMonitor(scheduler, job_key)
    if not monitor.wait_until(JobMonitor.terminal, instances=instances, with_timeout=True):
      context.print_err("Instances %s were not killed in time" % instances)
      return EXIT_TIMEOUT
    return EXIT_OK

  def kill_in_batches(self, context, job, instances_arg, config):
    api = context.get_api(job.cluster)
    # query the job, to get the list of active tasks.
    tasks = context.get_active_tasks(job)
    if tasks is None or len(tasks) == 0:
      context.print_err("No tasks to kill found for job %s" % job)
      return EXIT_INVALID_PARAMETER
    instance_ids = set(instance.assignedTask.instanceId for instance in tasks)
    # intersect that with the set of shards specified by the user.
    instances_to_kill = (instance_ids & set(instances_arg) if instances_arg is not None
        else instance_ids)
    # kill the shard batches.
    errors = 0
    while len(instances_to_kill) > 0:
      batch = []
      for i in range(min(context.options.batch_size, len(instances_to_kill))):
        batch.append(instances_to_kill.pop())
      resp = api.kill_job(job, batch, config=config, message=context.options.message)
      # Short circuit max errors in this case as it's most likely a fatal repeatable error.
      context.log_response_and_raise(
        resp,
        err_msg="Kill of instances %s failed with error:" % batch)

      if self.wait_kill_tasks(context, api.scheduler_proxy, job, batch) != EXIT_OK:
        errors += 1
        if errors > context.options.max_total_failures:
          raise context.CommandError(EXIT_COMMAND_FAILURE,
               "Exceeded maximum number of errors while killing instances")
      else:
        context.print_out("Successfully killed instances %s" % batch)
    if errors > 0:
      raise context.CommandError(EXIT_COMMAND_FAILURE, "Errors occurred while killing instances")


class AddCommand(Verb):
  @property
  def name(self):
    return "add"

  @property
  def help(self):
    return textwrap.dedent("""\
        Add instances to a scheduled job. The task config to replicate is specified by the
        /INSTANCE value of the task_instance argument.""")

  def get_options(self):
    return [BROWSER_OPTION,
        BIND_OPTION,
        ADD_INSTANCE_WAIT_OPTION,
        CONFIG_OPTION,
        JSON_READ_OPTION,
        TASK_INSTANCE_ARGUMENT,
        CommandOption('instance_count', type=int, help='Number of instances to add.')]

  def execute(self, context):
    job = context.options.task_instance.jobkey
    instance = context.options.task_instance.instance
    count = context.options.instance_count

    active = context.get_active_instances_or_raise(job, [instance])
    start = max(list(active)) + 1

    api = context.get_api(job.cluster)
    resp = api.add_instances(job, instance, count)
    context.log_response_and_raise(resp)

    wait_until(context.options.wait_until, job, api, range(start, start + count))

    if context.options.open_browser:
      webbrowser.open_new_tab(get_job_page(api, job))

    return EXIT_OK


class KillCommand(AbstractKillCommand):
  @property
  def name(self):
    return "kill"

  @property
  def help(self):
    return "Kill instances in a scheduled job"

  def get_options(self):
    return super(KillCommand, self).get_options() + [INSTANCES_SPEC_ARGUMENT, STRICT_OPTION]

  def execute(self, context):
    job = context.options.instance_spec.jobkey
    instances_arg = context.options.instance_spec.instance
    if instances_arg == ALL_INSTANCES:
      raise context.CommandError(EXIT_INVALID_PARAMETER,
          "The instances list cannot be omitted in a kill command!; "
          "use killall to kill all instances")
    if context.options.strict:
      context.get_active_instances_or_raise(job, instances_arg)
    api = context.get_api(job.cluster)
    config = context.get_job_config_optional(job, context.options.config)
    if context.options.no_batching:
      resp = api.kill_job(job, instances_arg, config=config, message=context.options.message)
      context.log_response_and_raise(resp)
      wait_result = self.wait_kill_tasks(context, api.scheduler_proxy, job, instances_arg)
      if wait_result is not EXIT_OK:
        return wait_result
    else:
      self.kill_in_batches(context, job, instances_arg, config)
    if context.options.open_browser:
      webbrowser.open_new_tab(get_job_page(api, job))
    context.print_out("Job kill succeeded")
    return EXIT_OK


class KillAllJobCommand(AbstractKillCommand):
  @property
  def name(self):
    return "killall"

  @property
  def help(self):
    return "Kill all instances of a scheduled job"

  def get_options(self):
    return super(KillAllJobCommand, self).get_options() + [JOBSPEC_ARGUMENT]

  def execute(self, context):
    job = context.options.jobspec
    api = context.get_api(job.cluster)
    config = context.get_job_config_optional(job, context.options.config)
    if context.options.no_batching:
      resp = api.kill_job(job, None, config=config, message=context.options.message)
      context.log_response_and_raise(resp)
      wait_result = self.wait_kill_tasks(context, api.scheduler_proxy, job)
      if wait_result is not EXIT_OK:
        return wait_result
    else:
      self.kill_in_batches(context, job, None, config)
    if context.options.open_browser:
      webbrowser.open_new_tab(get_job_page(api, job))
    context.print_out("Job killall succeeded")
    return EXIT_OK


class ListJobsCommand(Verb):

  @property
  def help(self):
    return "List jobs that match a jobkey or jobkey pattern."

  @property
  def name(self):
    return "list"

  def get_options(self):
    return [WILDCARD_JOBKEY_OPTION]

  def execute(self, context):
    jobs = context.get_jobs_matching_key(context.options.jobspec)
    for j in jobs:
      context.print_out("%s/%s/%s/%s" % (j.cluster, j.role, j.env, j.name))
    return EXIT_OK


class OpenCommand(Verb):
  @property
  def name(self):
    return "open"

  @property
  def help(self):
    return "Open a job's scheduler page in the web browser."

  def get_options(self):
    return [
      CommandOption("key", type=str, metavar="cluster[/role[/env[/job]]]",
        help="A key for the cluster, role, env, or job whose scheduler page should be opened.")
    ]

  def execute(self, context):
    key_parts = context.options.key.split("/")
    while len(key_parts) < 4:
      key_parts.append(None)
    (cluster, role, env, name) = key_parts
    api = context.get_api(cluster)
    webbrowser.open_new_tab(
      synthesize_url(api.scheduler_proxy.scheduler_client().url, role, env, name)
    )
    return EXIT_OK


class RestartCommand(Verb):
  @property
  def name(self):
    return "restart"

  def get_options(self):
    return [
        BATCH_OPTION,
        BIND_OPTION,
        BROWSER_OPTION,
        CONFIG_OPTION,
        FORCE_OPTION,
        HEALTHCHECK_OPTION,
        INSTANCES_SPEC_ARGUMENT,
        JSON_READ_OPTION,
        MAX_TOTAL_FAILURES_OPTION,
        STRICT_OPTION,
        WATCH_OPTION,
        CommandOption("--max-per-instance-failures", type=int, default=0,
             help="Maximum number of restarts per instance during restart. Increments total "
                  "failure count when this limit is exceeded.")]

  @property
  def help(self):
    return textwrap.dedent("""\
        Perform a rolling restart of instances within a job.
        Restarts are fully controlled client-side, so aborting halts the restart.""")

  def execute(self, context):
    # Check for negative max_total_failures option - negative is an error.
    # for per-instance failures, negative means "no limit", so it's allowed.
    if context.options.max_total_failures < 0:
      context.print_err("max_total_failures option must be >0, but you specified %s" %
          context.options.max_total_failures)
      return EXIT_INVALID_PARAMETER

    job = context.options.instance_spec.jobkey
    instances = (None if context.options.instance_spec.instance == ALL_INSTANCES else
        context.options.instance_spec.instance)
    if instances is not None and context.options.strict:
      context.get_active_instances_or_raise(job, instances)
    api = context.get_api(job.cluster)
    config = context.get_job_config_optional(job, context.options.config)
    restart_settings = RestartSettings(
        batch_size=context.options.batch_size,
        watch_secs=context.options.watch_secs,
        max_per_instance_failures=context.options.max_per_instance_failures,
        max_total_failures=context.options.max_total_failures,
        health_check_interval_seconds=context.options.healthcheck_interval_seconds)
    resp = api.restart(job, instances, restart_settings, config=config)

    context.log_response_and_raise(resp,
                                   err_msg="Error restarting job %s:" % str(job))
    context.print_out("Job %s restarted successfully" % str(job))
    if context.options.open_browser:
      webbrowser.open_new_tab(get_job_page(api, job))
    return EXIT_OK


class StatusCommand(Verb):

  @property
  def help(self):
    return textwrap.dedent("""\
        Get status information about a scheduled job or group of jobs.
        The jobspec parameter can omit parts of the jobkey, or use shell-style globs.""")

  @property
  def name(self):
    return "status"

  def get_options(self):
    return [JSON_WRITE_OPTION, WILDCARD_JOBKEY_OPTION]

  def render_tasks_json(self, jobkey, active_tasks, inactive_tasks):
    """Render the tasks running for a job in machine-processable JSON format."""
    def render_task_json(scheduled_task):
      """Render a single task into json. This is baroque, but it uses thrift to
      give us all of the job status data, while allowing us to compose it with
      other stuff and pretty-print it.
      """
      task = json.loads(serialize(scheduled_task,
          protocol_factory=TJSONProtocol.TSimpleJSONProtocolFactory()))
      # Now, clean it up: take all fields that are actually enums, and convert
      # their values to strings.
      task["status"] = ScheduleStatus._VALUES_TO_NAMES[task["status"]]
      events = sorted(task["taskEvents"], key=lambda event: event["timestamp"])
      for event in events:
        event["status"] = ScheduleStatus._VALUES_TO_NAMES[event["status"]]
      # convert boolean fields to boolean value names.
      assigned = task["assignedTask"]
      task_config = assigned["task"]
      task_config["isService"] = (task_config["isService"] != 0)
      if "production" in task_config:
        task_config["production"] = (task_config["production"] != 0)
      return task

    return {"job": str(jobkey),
        "active": [render_task_json(task) for task in active_tasks],
        "inactive": [render_task_json(task) for task in inactive_tasks]}

  def render_tasks_pretty(self, jobkey, active_tasks, inactive_tasks):
    """Render the tasks for a job in human-friendly format"""
    def render_task_pretty(scheduled_task):
      assigned_task = scheduled_task.assignedTask
      task_info = assigned_task.task
      task_strings = []
      task_strings.append("\tTask role: %s, env: %s, name: %s, instance: %s, status: %s on %s" %
             (task_info.job.role,
              task_info.job.environment,
              task_info.job.name,
              assigned_task.instanceId,
              ScheduleStatus._VALUES_TO_NAMES[scheduled_task.status],
              assigned_task.slaveHost))

      resource_details = ResourceManager.resource_details_from_task(task_info)
      if task_info:
        task_strings.append("""\t  %s""" % ", ".join("%s: %s%s" % (
            r.resource_type.display_name,
            r.value,
            r.resource_type.display_unit) for r in resource_details))

      if assigned_task.assignedPorts:
        task_strings.append("\t  assigned ports: %s" % assigned_task.assignedPorts)
        # TODO(mchucarroll): only add the max if taskInfo is filled in!
        task_strings.append("\t  failure count: %s (max %s)" % (scheduled_task.failureCount,
            task_info.maxTaskFailures))
      task_strings.append("\t  events:")
      events = sorted(scheduled_task.taskEvents, key=lambda event: event.timestamp)
      for event in events:
        task_strings.append("\t   %s %s: %s" % (datetime.fromtimestamp(event.timestamp / 1000),
            ScheduleStatus._VALUES_TO_NAMES[event.status], event.message))
      if assigned_task.task.metadata is not None and len(assigned_task.task.metadata) > 0:
        task_strings.append("\t  metadata:")
        for md in assigned_task.task.metadata:
          task_strings.append("\t\t  (key: '%s', value: '%s')" % (md.key, md.value))
      task_strings.append("")
      return "\n".join(task_strings)

    result = ["Active tasks (%s):\n" % len(active_tasks)]
    for t in active_tasks:
      result.append(render_task_pretty(t))
    result.append("Inactive tasks (%s):\n" % len(inactive_tasks))
    for t in inactive_tasks:
      result.append(render_task_pretty(t))
    return "".join(result)

  def _get_job_status(self, key, context):
    """Returns a list of task instances."""
    api = context.get_api(key.cluster)
    resp = api.check_status(key)
    context.log_response_and_raise(resp, err_code=EXIT_INVALID_PARAMETER)
    return resp.result.scheduleStatusResult.tasks

  def get_status_for_jobs(self, jobkeys, context):
    """Retrieve and render the status information for a collection of jobs"""
    def is_active(task):
      return task.status in ACTIVE_STATES

    result = []
    for jk in jobkeys:
      job_tasks = self._get_job_status(jk, context)
      if not job_tasks:
        logging.info("No tasks were found for jobkey %s" % jk)
        continue
      active_tasks = sorted([t for t in job_tasks if is_active(t)],
                            key=lambda task: task.assignedTask.instanceId)
      inactive_tasks = sorted([t for t in job_tasks if not is_active(t)],
                              key=lambda task: task.assignedTask.instanceId)
      if context.options.write_json:
        result.append(self.render_tasks_json(jk, active_tasks, inactive_tasks))
      else:
        result.append(self.render_tasks_pretty(jk, active_tasks, inactive_tasks))
    if result == []:
      return None
    if context.options.write_json:
      return json.dumps(result, indent=2, separators=[",", ": "], sort_keys=False)
    else:
      return "".join(result)

  @classmethod
  def _render_partial_jobkey(cls, jobkey):
    return "%s/%s/%s/%s" % jobkey

  @classmethod
  def _print_jobs_not_found(cls, context):
    if context.options.write_json:
      context.print_out(json.dumps(
        {"jobspec": cls._render_partial_jobkey(context.options.jobspec),
         "error": "No matching jobs found"},
        separators=[",", ":"],
        sort_keys=False))
      # If users are scripting, they don't want an error code if they were returned
      # a valid json answer.
      return EXIT_OK
    else:
      context.print_err("Found no jobs matching %s" %
          cls._render_partial_jobkey(context.options.jobspec))
      return EXIT_INVALID_PARAMETER

  def execute(self, context):
    jobs = context.get_jobs_matching_key(context.options.jobspec)
    if not jobs:
      return self._print_jobs_not_found(context)

    result = self.get_status_for_jobs(jobs, context)
    if result is not None:
      context.print_out(result)
      return EXIT_OK
    else:
      return self._print_jobs_not_found(context)


class Job(Noun):
  @property
  def name(self):
    return "job"

  @property
  def help(self):
    return "Work with an aurora job"

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(Job, self).__init__()
    self.register_verb(CreateJobCommand())
    self.register_verb(DiffCommand())
    self.register_verb(InspectCommand())
    self.register_verb(KillCommand())
    self.register_verb(KillAllJobCommand())
    self.register_verb(ListJobsCommand())
    self.register_verb(OpenCommand())
    self.register_verb(RestartCommand())
    self.register_verb(StatusCommand())
    self.register_verb(AddCommand())
