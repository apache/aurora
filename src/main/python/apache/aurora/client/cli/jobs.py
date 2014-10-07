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
import os
import pprint
import subprocess
import textwrap
import time
from datetime import datetime
from tempfile import NamedTemporaryFile

from thrift.protocol import TJSONProtocol
from thrift.TSerialization import serialize

from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.api.updater_util import UpdaterConfig
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
from apache.aurora.client.cli.options import (
    ALL_INSTANCES,
    BATCH_OPTION,
    BIND_OPTION,
    BROWSER_OPTION,
    CommandOption,
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
    WATCH_OPTION
)
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.api.constants import ACTIVE_STATES, AURORA_EXECUTOR_NAME
from gen.apache.aurora.api.ttypes import ExecutorConfig, ResponseCode, ScheduleStatus


def arg_type_jobkey(key):
  return AuroraCommandContext.parse_partial_jobkey(key)
WILDCARD_JOBKEY_OPTION = CommandOption('jobspec', type=arg_type_jobkey,
        metavar="cluster[/role[/env[/name]]]",
        help="A jobkey, optionally containing wildcards")


class CancelUpdateCommand(Verb):
  @property
  def name(self):
    return "cancel-update"

  @property
  def help(self):
    return "Cancel an in-progress update operation, releasing the update lock"

  def get_options(self):
    return [JSON_READ_OPTION,
        CommandOption("--config", type=str, default=None, dest="config_file",
            metavar="pathname",
            help="Config file for the job, possibly containing hooks"),
        JOBSPEC_ARGUMENT]

  def execute(self, context):
    api = context.get_api(context.options.jobspec.cluster)
    config = (context.get_job_config(context.options.jobspec, context.options.config_file)
        if context.options.config_file else None)
    resp = api.cancel_update(context.options.jobspec, config=config)
    context.check_and_log_response(resp)
    return EXIT_OK


class CreateJobCommand(Verb):
  @property
  def name(self):
    return "create"

  @property
  def help(self):
    return "Create a job using aurora"

  CREATE_STATES = ("PENDING", "RUNNING", "FINISHED")

  def get_options(self):
    return [BIND_OPTION, JSON_READ_OPTION,
        CommandOption("--wait-until", choices=self.CREATE_STATES,
            default="PENDING",
            help=("Block the client until all the tasks have transitioned into the requested "
                "state. Default: PENDING")),
        BROWSER_OPTION,
        JOBSPEC_ARGUMENT, CONFIG_ARGUMENT]

  def execute(self, context):
    config = context.get_job_config(context.options.jobspec, context.options.config_file)
    api = context.get_api(config.cluster())
    resp = api.create_job(config)
    context.log_response(resp)
    if resp.responseCode == ResponseCode.INVALID_REQUEST:
      context.print_err("job create failed because job not found")
      raise context.CommandError(EXIT_INVALID_PARAMETER, "Job not found")
    elif resp.responseCode == ResponseCode.ERROR:
      context.print_err("job create failed because of scheduler error")
      raise context.CommandError(EXIT_COMMAND_FAILURE,
          "Error reported by scheduler; see log for details")
    if context.options.open_browser:
      context.open_job_page(api, config)
    if context.options.wait_until == "RUNNING":
      JobMonitor(api.scheduler_proxy, config.job_key()).wait_until(JobMonitor.running_or_finished)
    elif context.options.wait_until == "FINISHED":
      JobMonitor(api.scheduler_proxy, config.job_key()).wait_until(JobMonitor.terminal)
    context.print_out("job create succeeded: job url=%s" %
        context.get_job_page(api, context.options.jobspec))
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
        alternate diff program by setting the DIFF_VIEWER environment variable.""")

  @property
  def name(self):
    return "diff"

  def get_options(self):
    return [BIND_OPTION, JSON_READ_OPTION,
        CommandOption("--from", dest="rename_from", type=AuroraJobKey.from_path, default=None,
            metavar="cluster/role/env/name",
            help="If specified, the job key to diff against."),
        JOBSPEC_ARGUMENT, CONFIG_ARGUMENT]

  def pretty_print_task(self, task):
    task.configuration = None
    task.executorConfig = ExecutorConfig(
        name=AURORA_EXECUTOR_NAME,
        data=json.loads(task.executorConfig.data))
    return self.prettyprinter.pformat(vars(task))

  def pretty_print_tasks(self, tasks):
    return ",\n".join(self.pretty_print_task(t) for t in tasks)

  def dump_tasks(self, tasks, out_file):
    out_file.write(self.pretty_print_tasks(tasks))
    out_file.write("\n")
    out_file.flush()

  def execute(self, context):
    config = context.get_job_config(context.options.jobspec, context.options.config_file)
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
    resp = api.query(api.build_query(role, name, statuses=ACTIVE_STATES, env=env))
    context.check_and_log_response(resp, err_code=EXIT_INVALID_PARAMETER,
        err_msg="Could not find job to diff against")
    if resp.result.scheduleStatusResult.tasks is None:
      context.print_err("No tasks found for job %s" % context.options.jobspec)
      return EXIT_COMMAND_FAILURE
    else:
      remote_tasks = [t.assignedTask.task for t in resp.result.scheduleStatusResult.tasks]
    resp = api.populate_job_config(config)
    context.check_and_log_response(resp, err_code=EXIT_INVALID_CONFIGURATION,
          err_msg="Error loading configuration; see log for details")
    local_tasks = resp.result.populateJobResult.populatedDEPRECATED
    diff_program = os.environ.get("DIFF_VIEWER", "diff")
    with NamedTemporaryFile() as local:
      self.dump_tasks(local_tasks, local)
      with NamedTemporaryFile() as remote:
        self.dump_tasks(remote_tasks, remote)
        result = subprocess.call([diff_program, remote.name, local.name])
        # Unlike most commands, diff doesn't return zero on success; it returns
        # 1 when a successful diff is non-empty.
        if result not in (0, 1):
          raise context.CommandError(EXIT_COMMAND_FAILURE, "Error running diff command")
        else:
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
    return [BIND_OPTION, JSON_READ_OPTION,
        CommandOption("--local", dest="local", default=False, action="store_true",
            help='Inspect the configuration as would be created by the "job create" command.'),
        CommandOption("--raw", dest="raw", default=False, action="store_true",
            help="Show the raw configuration."),
        JOBSPEC_ARGUMENT, CONFIG_ARGUMENT]

  def execute(self, context):
    config = context.get_job_config(context.options.jobspec, context.options.config_file)
    if context.options.raw:
      context.print_out(config.job())
      return EXIT_OK

    job = config.raw()
    job_thrift = config.job()
    context.print_out("Job level information")
    context.print_out("name:       '%s'" % job.name(), indent=2)
    context.print_out("role:       '%s'" % job.role(), indent=2)
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


class AbstractKillCommand(Verb):
  def get_options(self):
    return [BROWSER_OPTION,
        CommandOption("--config", type=str, default=None, dest="config",
            metavar="pathname",
            help="Config file for the job, possibly containing hooks"),
        BATCH_OPTION,
        MAX_TOTAL_FAILURES_OPTION,
        NO_BATCHING_OPTION]

  def wait_kill_tasks(self, context, scheduler, job_key, instances=None):
    monitor = JobMonitor(scheduler, job_key)
    if not monitor.wait_until(JobMonitor.terminal, instances=instances, with_timeout=True):
      context.print_err("Tasks were not killed in time.")
      return EXIT_TIMEOUT
    return EXIT_OK

  def kill_in_batches(self, context, job, instances_arg):
    api = context.get_api(job.cluster)
    # query the job, to get the list of active instances.
    tasks = context.get_job_status(job)
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
      resp = api.kill_job(job, batch)
      context.log_response(resp)
      if resp.responseCode is not ResponseCode.OK or self.wait_kill_tasks(
          context, api.scheduler_proxy, job, batch) is not EXIT_OK:
        context.print_err("Kill of shards %s failed with error; see log for details" % batch)
        errors += 1
        if errors > context.options.max_total_failures:
          context.print_err("Exceeded maximum number of errors while killing instances")
          raise context.CommandError(EXIT_COMMAND_FAILURE,
               "Exceeded maximum number of errors while killing instances")
      else:
        context.print_out("Successfully killed shards %s" % batch)
    if errors > 0:
      context.print_err("Warning: Errors occurred during batch kill")
      raise context.CommandError(EXIT_COMMAND_FAILURE, "Errors occurred while killing instances")


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
      context.verify_shards_option_validity(job, instances_arg)
    api = context.get_api(job.cluster)
    if context.options.no_batching:
      resp = api.kill_job(job, instances_arg)
      context.check_and_log_response(resp)
      wait_result = self.wait_kill_tasks(context, api.scheduler_proxy, job, instances_arg)
      if wait_result is not EXIT_OK:
        return wait_result
    else:
      self.kill_in_batches(context, job, instances_arg)
    if context.options.open_browser:
      context.open_job_page(api, context.options.jobspec)
    context.print_out("job kill succeeded")
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
    if context.options.no_batching:
      resp = api.kill_job(job, None)
      context.check_and_log_response(resp)
      wait_result = self.wait_kill_tasks(context, api.scheduler_proxy, job)
      if wait_result is not EXIT_OK:
        return wait_result
    else:
      self.kill_in_batches(context, job, None)
    if context.options.open_browser:
      context.open_job_page(api, job)
    context.print_out("job killall succeeded")
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
    context.open_scheduler_page(cluster, role, env, name)
    return EXIT_OK


class RestartCommand(Verb):
  @property
  def name(self):
    return "restart"

  def get_options(self):
    return [BATCH_OPTION, BIND_OPTION, BROWSER_OPTION, FORCE_OPTION, HEALTHCHECK_OPTION,
        JSON_READ_OPTION, WATCH_OPTION,
        CommandOption('--max-per-instance-failures', type=int, default=0,
             help='Maximum number of restarts per instance during restart. Increments total '
                  'failure count when this limit is exceeded.'),
        CommandOption('--restart-threshold', type=int, default=60,
             help='Maximum number of seconds before a shard must move into the RUNNING state '
                  'before considered a failure.'),
        CONFIG_OPTION,
        MAX_TOTAL_FAILURES_OPTION,
        STRICT_OPTION,
        CommandOption("--rollback-on-failure", default=True, action="store_false",
            help="If false, prevent update from performing a rollback."),
        INSTANCES_SPEC_ARGUMENT]

  @property
  def help(self):
    return textwrap.dedent("""\
        Perform a rolling restart of shards within a job.
        Restarts are fully controlled client-side, so aborting halts the restart.""")

  def execute(self, context):
    # Check for negative max_total_failures option - negative is an error.
    # for per-shard failures, negative means "no limit", so it's allowed.
    if context.options.max_total_failures < 0:
      context.print_err("max_total_failures option must be >0, but you specified %s" %
          context.options.max_total_failures)
      context.print_log(logging.INFO, "Error: max_total_failures option=%s" %
          context.options.max_total_failures)
      return EXIT_INVALID_PARAMETER

    job = context.options.instance_spec.jobkey
    instances = (None if context.options.instance_spec.instance == ALL_INSTANCES else
        context.options.instance_spec.instance)
    if instances is not None and context.options.strict:
      context.verify_shards_option_validity(job, instances)
    api = context.get_api(job.cluster)
    config = (context.get_job_config(job, context.options.config)
        if context.options.config else None)
    updater_config = UpdaterConfig(
        context.options.batch_size,
        context.options.restart_threshold,
        context.options.watch_secs,
        context.options.max_per_instance_failures,
        context.options.max_total_failures,
        context.options.rollback_on_failure)
    resp = api.restart(job, instances, updater_config,
        context.options.healthcheck_interval_seconds, config=config)

    if resp.responseCode != ResponseCode.OK:
      context.print_err("Error restarting job %s; see log for details" % str(job))
    else:
      context.print_out("Job %s restarted successfully" % str(job))
    context.check_and_log_response(resp)
    if context.options.open_browser:
      context.open_job_page(api, context.options.jobspec)
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
      task['status'] = ScheduleStatus._VALUES_TO_NAMES[task['status']]
      for event in task['taskEvents']:
        event['status'] = ScheduleStatus._VALUES_TO_NAMES[event['status']]
      # convert boolean fields to boolean value names.
      assigned = task['assignedTask']
      task_config = assigned['task']
      task_config['isService'] = (task_config['isService'] != 0)
      if 'production' in task_config:
        task_config['production'] = (task_config['production'] != 0)
      return task

    return {"job": str(jobkey),
        "active": [render_task_json(task) for task in active_tasks],
        "inactive": [render_task_json(task) for task in inactive_tasks]}

  def render_tasks_pretty(self, jobkey, active_tasks, inactive_tasks):
    """Render the tasks for a job in human-friendly format"""
    def render_task_pretty(scheduled_task):
      assigned_task = scheduled_task.assignedTask
      task_info = assigned_task.task
      task_strings = ["\tTask:"]
      if task_info:
        task_strings.append("""\t  cpus: %s, ram: %s MB, disk: %s MB""" % (
            task_info.numCpus, task_info.ramMb, task_info.diskMb))
      if assigned_task.assignedPorts:
        task_strings.append("\t  ports: %s" % assigned_task.assignedPorts)
        # TODO(mchucarroll): only add the max if taskInfo is filled in!
        task_strings.append("\t  failure count: %s (max %s)" % (scheduled_task.failureCount,
            task_info.maxTaskFailures))
      task_strings.append("\t  events:")
      for event in scheduled_task.taskEvents:
        task_strings.append("\t   %s %s: %s" % (datetime.fromtimestamp(event.timestamp / 1000),
            ScheduleStatus._VALUES_TO_NAMES[event.status], event.message))
      if assigned_task.task.metadata is not None and len(assigned_task.task.metadata) > 0:
        task_strings.append("\t  metadata:")
        for md in assigned_task.task.metadata:
          task_strings.append("\t\t  (key: '%s', value: '%s')" % (md.key, md.value))
      task_strings.append('')
      return "\n".join(task_strings)

    result = ["Active tasks (%s):\n" % len(active_tasks)]
    for t in active_tasks:
      result.append(render_task_pretty(t))
    result.append("Inactive tasks (%s):\n" % len(inactive_tasks))
    for t in inactive_tasks:
      result.append(render_task_pretty(t))
    return "".join(result)

  def get_status_for_jobs(self, jobkeys, context):
    """Retrieve and render the status information for a collection of jobs"""
    def is_active(task):
      return task.status in ACTIVE_STATES

    result = []
    for jk in jobkeys:
      job_tasks = context.get_job_status(jk)
      if job_tasks is None or job_tasks is []:
        context.print_log(logging.INFO, "No tasks were found for jobkey %s" % jk)
        continue
      active_tasks = [t for t in job_tasks if is_active(t)]
      inactive_tasks = [t for t in job_tasks if not is_active(t)]
      if context.options.write_json:
        result.append(self.render_tasks_json(jk, active_tasks, inactive_tasks))
      else:
        result.append(self.render_tasks_pretty(jk, active_tasks, inactive_tasks))
    if result == []:
      return None
    if context.options.write_json:
      return json.dumps(result, indent=2, separators=[',', ': '], sort_keys=False)
    else:
      return ''.join(result)

  def _print_jobs_not_found(self, context):
    if context.options.write_json:
      context.print_out(json.dumps(
        {"jobspec": context.render_partial_jobkey(context.options.jobspec),
         "error": "No matching jobs found"},
        separators=[",", ":"],
        sort_keys=False))
      # If users are scripting, they don't want an error code if they were returned
      # a valid json answer.
      return EXIT_OK
    else:
      context.print_err("Found no jobs matching %s" %
          context.render_partial_jobkey(context.options.jobspec))
      return EXIT_INVALID_PARAMETER

  def execute(self, context):
    jobs = context.get_jobs_matching_key(context.options.jobspec)
    if jobs is None or jobs == []:
      return self._print_jobs_not_found()

    result = self.get_status_for_jobs(jobs, context)
    if result is not None:
      context.print_out(result)
      return EXIT_OK
    else:
      return self._print_jobs_not_found(context)


class UpdateCommand(Verb):
  @property
  def name(self):
    return 'update'

  def get_options(self):
    return [FORCE_OPTION, BIND_OPTION, JSON_READ_OPTION, HEALTHCHECK_OPTION,
        INSTANCES_SPEC_ARGUMENT, STRICT_OPTION, CONFIG_ARGUMENT]

  @property
  def help(self):
    return textwrap.dedent("""\
        Perform a rolling upgrade on a running job, using the update configuration
        within the config file as a control for update velocity and failure tolerance.

        Updates are fully controlled client-side, so aborting an update halts the
        update and leaves the job in a 'locked' state on the scheduler.
        Subsequent update attempts will fail until the update is 'unlocked' using the
        'cancel_update' command.

        The updater only takes action on instances in a job that have changed, meaning
        that changing a single instance will only induce a restart on the changed task instance.

        You may want to consider using the 'diff' subcommand before updating,
        to preview what changes will take effect.
        """)

  def warn_if_dangerous_change(self, context, api, job_spec, config):
    # Get the current job status, so that we can check if there's anything
    # dangerous about this update.
    resp = api.query_no_configs(api.build_query(config.role(), config.name(),
        statuses=ACTIVE_STATES, env=config.environment()))
    if resp.responseCode != ResponseCode.OK:
      # NOTE(mchucarroll): we assume here that updating a cron schedule and updating a
      # running job are different operations; in client v1, they were both done with update.
      raise context.CommandError(EXIT_COMMAND_FAILURE,
          "Server could not find running job to update: see log for details")
    remote_tasks = [t.assignedTask.task for t in resp.result.scheduleStatusResult.tasks]
    resp = api.populate_job_config(config)
    context.check_and_log_response(resp, err_code=EXIT_COMMAND_FAILURE,
        err_msg="Server could not populate job config for comparison; see log for details.")
    local_task_count = len(resp.result.populateJobResult.populatedDEPRECATED)
    remote_task_count = len(remote_tasks)

    # Dangerous if it's more than a factor-of-four change in number of instances.
    if (local_task_count >= 4 * remote_task_count or
        4 * local_task_count <= remote_task_count or
        local_task_count == 0):
      context.print_out("Warning: this update is a large change. "
          "Press ^C within 5 seconds to abort")
      time.sleep(5)

  def execute(self, context):
    job = context.options.instance_spec.jobkey
    instances = (None if context.options.instance_spec.instance == ALL_INSTANCES else
        context.options.instance_spec.instance)
    if instances is not None and context.options.strict:
      context.verify_shards_option_validity(job, instances)
    config = context.get_job_config(job, context.options.config_file)
    api = context.get_api(config.cluster())
    if not context.options.force:
      self.warn_if_dangerous_change(context, api, job, config)
    resp = api.update_job(config, context.options.healthcheck_interval_seconds,
        instances)
    context.check_and_log_response(resp, err_code=EXIT_COMMAND_FAILURE,
        err_msg="Update failed; see log for details.")
    context.print_out("Update completed successfully")
    return EXIT_OK


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
    self.register_verb(CancelUpdateCommand())
    self.register_verb(CreateJobCommand())
    self.register_verb(DiffCommand())
    self.register_verb(InspectCommand())
    self.register_verb(KillCommand())
    self.register_verb(KillAllJobCommand())
    self.register_verb(ListJobsCommand())
    self.register_verb(OpenCommand())
    self.register_verb(RestartCommand())
    self.register_verb(StatusCommand())
    self.register_verb(UpdateCommand())
