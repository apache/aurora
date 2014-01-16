from __future__ import print_function
from datetime import datetime
import json
import os
import pprint
import subprocess
import sys
from tempfile import NamedTemporaryFile

from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.cli import (
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_CONFIGURATION,
    EXIT_INVALID_PARAMETER,
    EXIT_OK,
    Noun,
    Verb,
)
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    BIND_OPTION,
    BROWSER_OPTION,
    CONFIG_ARGUMENT,
    JOBSPEC_ARGUMENT,
    JSON_OPTION,
)
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.constants import ACTIVE_STATES, AURORA_EXECUTOR_NAME
from gen.apache.aurora.ttypes import (
    ExecutorConfig,
    ResponseCode,
    ScheduleStatus,
)

from pystachio.config import Config
from thrift.TSerialization import serialize
from thrift.protocol import TJSONProtocol


def parse_instances(instances):
  """Parse lists of instances or instance ranges into a set().
     Examples:
       0-2
       0,1-3,5
       1,3,5
  """
  if instances is None or instances == '':
    return None
  result = set()
  for part in instances.split(','):
    x = part.split('-')
    result.update(range(int(x[0]), int(x[-1]) + 1))
  return sorted(result)


def arg_type_jobkey(key):
  return AuroraCommandContext.parse_partial_jobkey(key)


class CreateJobCommand(Verb):
  @property
  def name(self):
    return 'create'

  @property
  def help(self):
    return '''Usage: aurora create cluster/role/env/job config.aurora

    Create a job using aurora
    '''

  CREATE_STATES = ('PENDING', 'RUNNING', 'FINISHED')

  def setup_options_parser(self, parser):
    self.add_option(parser, BIND_OPTION)
    self.add_option(parser, BROWSER_OPTION)
    self.add_option(parser, JSON_OPTION)
    parser.add_argument('--wait_until', choices=self.CREATE_STATES,
        default='PENDING',
        help=('Block the client until all the tasks have transitioned into the requested state. '
                        'Default: PENDING'))
    self.add_option(parser, JOBSPEC_ARGUMENT)
    self.add_option(parser, CONFIG_ARGUMENT)

  def execute(self, context):
    try:
      config = context.get_job_config(context.options.jobspec, context.options.config_file)
    except Config.InvalidConfigError as e:
      raise context.CommandError(EXIT_INVALID_CONFIGURATION,
          'Error loading job configuration: %s' % e)
    api = context.get_api(config.cluster())
    monitor = JobMonitor(api, config.role(), config.environment(), config.name())
    resp = api.create_job(config)
    if resp.responseCode == ResponseCode.INVALID_REQUEST:
      raise context.CommandError(EXIT_INVALID_PARAMETER, 'Job not found')
    elif resp.responseCode == ResponseCode.ERROR:
      raise context.CommandError(EXIT_COMMAND_FAILURE, resp.message)
    if context.options.open_browser:
      context.open_job_page(api, config)
    if context.options.wait_until == 'RUNNING':
      monitor.wait_until(monitor.running_or_finished)
    elif context.options.wait_until == 'FINISHED':
      monitor.wait_until(monitor.terminal)
    return EXIT_OK


class KillJobCommand(Verb):
  @property
  def name(self):
    return 'kill'

  @property
  def help(self):
    return '''Usage: kill cluster/role/env/job

    Kill a scheduled job
    '''

  def setup_options_parser(self, parser):
    self.add_option(parser, BROWSER_OPTION)
    parser.add_argument('--instances', type=parse_instances, dest='instances', default=None,
        help='A list of instance ids to act on. Can either be a comma-separated list (e.g. 0,1,2) '
            'or a range (e.g. 0-2) or any combination of the two (e.g. 0-2,5,7-9). If not set, '
            'all instances will be acted on.')
    parser.add_argument('--config', type=str, default=None, dest='config',
         help='Config file for the job, possibly containing hooks')
    self.add_option(parser, JOBSPEC_ARGUMENT)

  def execute(self, context):
    # TODO: Check for wildcards; we don't allow wildcards for job kill.
    api = context.get_api(context.options.jobspec.cluster)
    resp = api.kill_job(context.options.jobspec, context.options.instances)
    if resp.responseCode != ResponseCode.OK:
      context.print_err('Job %s not found' % context.options.jobspec, file=sys.stderr)
      return EXIT_INVALID_PARAMETER
    if context.options.open_browser:
      context.open_job_page(api, context.options.jobspec)


class StatusCommand(Verb):
  @property
  def help(self):
    return '''Usage: aurora status jobspec

    Get status information about a scheduled job or group of jobs. The
    jobspec parameter can ommit parts of the jobkey, or use shell-style globs.
    '''

  @property
  def name(self):
    return 'status'

  def setup_options_parser(self, parser):
    parser.add_argument('--json', default=False, action='store_true',
        help='Show status information in machine-processable JSON format')
    parser.add_argument('jobspec', type=arg_type_jobkey)

  def render_tasks_json(self, jobkey, active_tasks, inactive_tasks):
    """Render the tasks running for a job in machine-processable JSON format."""
    def render_task_json(scheduled_task):
      """Render a single task into json. This is baroque, but it uses thrift to
      give us all of the job status data, while allowing us to compose it with
      other stuff and pretty-print it.
      """
      return json.loads(serialize(scheduled_task,
          protocol_factory=TJSONProtocol.TSimpleJSONProtocolFactory()))

    return {'job': str(jobkey),
        'active': [render_task_json(task) for task in active_tasks],
        'inactive': [render_task_json(task) for task in inactive_tasks]}

  def render_tasks_pretty(self, jobkey, active_tasks, inactive_tasks):
    """Render the tasks for a job in human-friendly format"""
    def render_task_pretty(scheduled_task):
      assigned_task = scheduled_task.assignedTask
      task_info = assigned_task.task
      task_strings = []
      if task_info:
        task_strings.append('''\tcpus: %s, ram: %s MB, disk: %s MB''' % (
            task_info.numCpus, task_info.ramMb, task_info.diskMb))
      if assigned_task.assignedPorts:
        task_strings.append('ports: %s' % assigned_task.assignedPorts)
        # TODO: only add the max if taskInfo is filled in!
        task_strings.append('failure count: %s (max %s)' % (scheduled_task.failureCount,
            task_info.maxTaskFailures))
        task_strings.append('events:')
      for event in scheduled_task.taskEvents:
        task_strings.append('\t %s %s: %s' % (datetime.fromtimestamp(event.timestamp / 1000),
            ScheduleStatus._VALUES_TO_NAMES[event.status], event.message))
        task_strings.append('packages:')
        for pkg in assigned_task.task.packages:
          task_strings.append('\trole: %s, package: %s, version: %s' %
              (pkg.role, pkg.name, pkg.version))
      return '\n\t'.join(task_strings)

    result = ["Active tasks (%s):\n" % len(active_tasks)]
    for t in active_tasks:
      result.append(render_task_pretty(t))
    result.append("Inactive tasks (%s):\n" % len(inactive_tasks))
    for t in inactive_tasks:
      result.append(render_task_pretty(t))
    return ''.join(result)

  def get_status_for_jobs(self, jobkeys, context):
    """Retrieve and render the status information for a collection of jobs"""
    def is_active(task):
      return task.status in ACTIVE_STATES

    result = []
    for jk in jobkeys:
      job_tasks = context.get_job_status(jk)
      active_tasks = [t for t in job_tasks if is_active(t)]
      inactive_tasks = [t for t in job_tasks if not is_active(t)]
      if context.options.json:
        result.append(self.render_tasks_json(jk, active_tasks, inactive_tasks))
      else:
        result.append(self.render_tasks_pretty(jk, active_tasks, inactive_tasks))
    if context.options.json:
      return json.dumps(result, indent=2, separators=[',', ': '], sort_keys=False)
    else:
      return ''.join(result)

  def execute(self, context):
    jobs = context.get_jobs_matching_key(context.options.jobspec)
    result = self.get_status_for_jobs(jobs, context)
    context.print_out(result)


class DiffCommand(Verb):

  def __init__(self):
    super(DiffCommand, self).__init__()
    self.prettyprinter = pprint.PrettyPrinter(indent=2)

  @property
  def help(self):
    return """usage: diff cluster/role/env/job config

  Compares a job configuration against a running job.
  By default the diff will be displayed using 'diff', though you may choose an alternate
  diff program by setting the DIFF_VIEWER environment variable.
  """

  @property
  def name(self):
    return 'diff'

  def setup_options_parser(self, parser):
    self.add_option(parser, BIND_OPTION)
    self.add_option(parser, JSON_OPTION)
    parser.add_argument('--from', dest='rename_from', type=AuroraJobKey.from_path, default=None,
        help='If specified, the job key to diff against.')
    self.add_option(parser, JOBSPEC_ARGUMENT)
    self.add_option(parser, CONFIG_ARGUMENT)

  def pretty_print_task(self, task):
    task.configuration = None
    task.executorConfig = ExecutorConfig(
        name=AURORA_EXECUTOR_NAME,
        data=json.loads(task.executorConfig.data))
    return self.prettyprinter.pformat(vars(task))

  def pretty_print_tasks(self, tasks):
    return ',\n'.join(self.pretty_print_task(t) for t in tasks)

  def dump_tasks(self, tasks, out_file):
    out_file.write(self.pretty_print_tasks(tasks))
    out_file.write('\n')
    out_file.flush()

  def execute(self, context):
    try:
      config = context.get_job_config(context.options.jobspec, context.options.config_file)
    except Config.InvalidConfigError as e:
      raise context.CommandError(EXIT_INVALID_CONFIGURATION,
          'Error loading job configuration: %s' % e)
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
    if resp.responseCode != ResponseCode.OK:
      raise context.CommandError(EXIT_INVALID_PARAMETER, 'Could not find job to diff against')
    remote_tasks = [t.assignedTask.task for t in resp.result.scheduleStatusResult.tasks]
    resp = api.populate_job_config(config)
    if resp.responseCode != ResponseCode.OK:
      raise context.CommandError(EXIT_INVALID_CONFIGURATION,
          'Error loading configuration: %s' % resp.message)
    local_tasks = resp.result.populateJobResult.populated
    diff_program = os.environ.get('DIFF_VIEWER', 'diff')
    with NamedTemporaryFile() as local:
      self.dump_tasks(local_tasks, local)
      with NamedTemporaryFile() as remote:
        self.dump_tasks(remote_tasks, remote)
        result = subprocess.call([diff_program, remote.name, local.name])
        # Unlike most commands, diff doesn't return zero on success; it returns
        # 1 when a successful diff is non-empty.
        if result not in (0, 1):
          raise context.CommandError(EXIT_COMMAND_FAILURE, 'Error running diff command')
        else:
          return EXIT_OK


class InspectCommand(Verb):
  @property
  def help(self):
    return """usage: inspect cluster/role/env/job config

  Verifies that a job can be parsed from a configuration file, and displays
  the parsed configuration.
  """

  @property
  def name(self):
    return 'inspect'

  def setup_options_parser(self, parser):
    self.add_option(parser, BIND_OPTION)
    self.add_option(parser, JSON_OPTION)
    parser.add_argument('--local', dest='local', default=False, action='store_true',
        help='Inspect the configuration as would be created by the "spawn" command.')
    parser.add_argument('--raw', dest='raw', default=False, action='store_true',
        help='Show the raw configuration.')

    self.add_option(parser, JOBSPEC_ARGUMENT)
    self.add_option(parser, CONFIG_ARGUMENT)

  def execute(self, context):
    config = context.get_job_config(context.options.jobspec, context.options.config_file)
    if context.options.raw:
      print(config.job())
      return EXIT_OK
    job_thrift = config.job()
    job = config.raw()
    job_thrift = config.job()
    print('Job level information')
    print('  name:       %s' % job.name())
    print('  role:       %s' % job.role())
    print('  contact:    %s' % job.contact())
    print('  cluster:    %s' % job.cluster())
    print('  instances:  %s' % job.instances())
    if job.has_cron_schedule():
      print('  cron:')
      print('     schedule: %s' % job.cron_schedule())
      print('     policy:   %s' % job.cron_collision_policy())
    if job.has_constraints():
      print('  constraints:')
      for constraint, value in job.constraints().get().items():
        print('    %s: %s' % (constraint, value))
    print('  service:    %s' % job_thrift.taskConfig.isService)
    print('  production: %s' % bool(job.production().get()))
    print()

    task = job.task()
    print('Task level information')
    print('  name: %s' % task.name())
    if len(task.constraints().get()) > 0:
      print('  constraints:')
      for constraint in task.constraints():
        print('    %s' % (' < '.join(st.get() for st in constraint.order() or [])))
    print()

    processes = task.processes()
    for process in processes:
      print('Process %s:' % process.name())
      if process.daemon().get():
        print('  daemon')
      if process.ephemeral().get():
        print('  ephemeral')
      if process.final().get():
        print('  final')
      print('  cmdline:')
      for line in process.cmdline().get().splitlines():
        print('    ' + line)
      print()


class Job(Noun):
  @property
  def name(self):
    return 'job'

  @property
  def help(self):
    return "Work with an aurora job"

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(Job, self).__init__()
    self.register_verb(CreateJobCommand())
    self.register_verb(KillJobCommand())
    self.register_verb(StatusCommand())
    self.register_verb(DiffCommand())
    self.register_verb(InspectCommand())
