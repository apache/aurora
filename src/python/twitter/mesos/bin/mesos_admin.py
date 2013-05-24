"""Command-line client for managing admin-only interactions with the aurora scheduler.
"""

import collections
import optparse
import sys

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Data
from twitter.common.quantity.parse_simple import parse_data
from twitter.mesos.client.base import check_and_log_response, die, requires
from twitter.mesos.client.api import MesosClientAPI

from gen.twitter.mesos.constants import ACTIVE_STATES, TERMINAL_STATES
from gen.twitter.mesos.ttypes import (
    ResponseCode,
    ScheduleStatus,
    TaskQuery)


DEFAULT_USAGE_BANNER = """
mesos admin, used for admin-only commands, to interact with the aurora scheduler.

For questions contact mesos-team@twitter.com.
"""


CLUSTER_OPTION = optparse.Option(
    '--cluster',
    dest='cluster',
    default=None,
    help='Cluster to invoke the command against.')


@app.command
@app.command_option(CLUSTER_OPTION)
@app.command_option('--force', dest='force', default=False, action='store_true',
    help='Force expensive queries to run.')
@app.command_option('--shards', dest='shards', default=None,
    help='Only match given shards of a job.')
@app.command_option('--states', dest='states', default='RUNNING',
    help='Only match tasks with given state(s).')
@app.command_option('-l', '--listformat', dest='listformat',
    default="%role%/%jobName%/%shardId% %status%",
    help='Format string of job/task items to print out.')
# TODO(ksweeney): Allow query by environment here.
def query(args, options):
  """usage: query --cluster=CLUSTER [--shards=N[,N,...]]
                                    [--states=State[,State,...]]
                                    [role [job]]

  Query Mesos about jobs and tasks.
  """
  def _convert_fmt_string(fmtstr):
    import re
    def convert(match):
      return "%%(%s)s" % match.group(1)
    return re.sub(r'%(\w+)%', convert, fmtstr)

  def flatten_task(t, d={}):
    for key in t.__dict__.keys():
      val = getattr(t, key)
      try:
        val.__dict__.keys()
      except AttributeError:
        d[key] = val
      else:
        flatten_task(val, d)

    return d

  def map_values(d):
    default_value = lambda v: v
    mapping = {
      'status': lambda v: ScheduleStatus._VALUES_TO_NAMES[v],
    }
    return dict(
      (k, mapping.get(k, default_value)(v)) for (k, v) in d.items()
    )

  for state in options.states.split(','):
    if state not in ScheduleStatus._NAMES_TO_VALUES:
      msg = "Unknown state '%s' specified.  Valid states are:\n" % state
      msg += ','.join(ScheduleStatus._NAMES_TO_VALUES.keys())
      die(msg)

  # Role, Job, Shards, States, and the listformat
  role = args[0] if len(args) > 0 else None
  job = args[1] if len(args) > 1 else None
  shards = set(map(int, options.shards.split(','))) if options.shards else set()
  states = set(map(ScheduleStatus._NAMES_TO_VALUES.get, options.states.split(','))) if options.states else ACTIVE_STATES | TERMINAL_STATES
  listformat = _convert_fmt_string(options.listformat)

  #  Figure out "expensive" queries here and bone if they do not have --force
  #  - Does not specify role
  if role is None and not options.force:
    die('--force is required for expensive queries (no role specified)')

  #  - Does not specify job
  if job is None and not options.force:
    die('--force is required for expensive queries (no job specified)')

  #  - Specifies status outside of ACTIVE_STATES
  if not (states <= ACTIVE_STATES) and not options.force:
    die('--force is required for expensive queries (states outside ACTIVE states')

  api = MesosClientAPI(options.cluster, options.verbosity)
  query_info = api.query(api.build_query(role, job, shards=shards, statuses=states))
  if query_info.responseCode != ResponseCode.OK:
    die('Failed to query scheduler: %s' % query_info.message)
  if query_info.tasks is None:
    return

  try:
    for task in query_info.tasks:
      d = flatten_task(task)
      print listformat % map_values(d)
  except KeyError:
    msg = "Unknown key in format string.  Valid keys are:\n"
    msg += ','.join(d.keys())
    die(msg)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'cpu', 'ramMb', 'diskMb')
def set_quota(role, cpu_str, ram_mb_str, disk_mb_str):
  """usage: set_quota --cluster=CLUSTER role cpu ramMb diskMb

  Alters the amount of production quota allocated to a user.
  """
  try:
    cpu = float(cpu_str)
    ram_mb = int(ram_mb_str)
    disk_mb = int(disk_mb_str)
  except ValueError:
    log.error('Invalid value')

  options = app.get_options()
  resp = MesosClientAPI(options.cluster, options.verbosity).set_quota(role, cpu, ram_mb, disk_mb)
  check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'cpu', 'ram', 'disk')
def increase_quota(role, cpu_str, ram_str, disk_str):
  """usage: increase_quota --cluster=CLUSTER role cpu ram[unit] disk[unit]

  Increases the amount of production quota allocated to a user.
  """
  cpu = float(cpu_str)
  ram = parse_data(ram_str)
  disk = parse_data(disk_str)

  options = app.get_options()
  client = MesosClientAPI(options.cluster, options.verbosity == 'verbose')
  resp = client.get_quota(role)
  quota = resp.quota
  log.info('Current quota for %s:\n\tCPU\t%s\n\tRAM\t%s MB\n\tDisk\t%s MB' %
           (role, quota.numCpus, quota.ramMb, quota.diskMb))

  new_cpu = cpu + quota.numCpus
  new_ram = ram + Amount(quota.ramMb, Data.MB)
  new_disk = disk + Amount(quota.diskMb, Data.MB)

  log.info('Attempting to update quota for %s to\n\tCPU\t%s\n\tRAM\t%s MB\n\tDisk\t%s MB' %
           (role, new_cpu, new_ram.as_(Data.MB), new_disk.as_(Data.MB)))

  resp = client.set_quota(role, new_cpu, new_ram.as_(Data.MB), new_disk.as_(Data.MB))
  check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('task_id', 'state')
def force_task_state(task_id, state):
  """usage: force_task_state --cluster=CLUSTER task_id state

  Forcibly induces a state transition on a task.
  This should never be used under normal circumstances, and any use of this command
  should be accompanied by a bug indicating why it was necessary.
  """
  status = ScheduleStatus._NAMES_TO_VALUES.get(state)
  if status is None:
    log.error('Unrecognized status "%s", must be one of [%s]'
              % (state, ', '.join(ScheduleStatus._NAMES_TO_VALUES.keys())))
    sys.exit(1)

  options = app.get_options()
  resp = MesosClientAPI(options.cluster, options.verbosity).force_task_state(task_id, status)
  check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_backup_now():
  """usage: scheduler_backup_now --cluster=CLUSTER

  Immediately initiates a full storage backup.
  """
  options = app.get_options()
  check_and_log_response(MesosClientAPI(options.cluster, options.verbosity).perform_backup())


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_list_backups():
  """usage: scheduler_list_backups --cluster=CLUSTER

  Lists backups available for recovery.
  """
  options = app.get_options()
  resp = MesosClientAPI(options.cluster, options.verbosity).list_backups()
  check_and_log_response(resp)
  log.info('%s available backups:' % len(resp.backups))
  for backup in resp.backups:
    log.info(backup)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('backup_id')
def scheduler_stage_recovery(backup_id):
  """usage: scheduler_stage_recovery --cluster=CLUSTER backup_id

  Stages a backup for recovery.
  """
  options = app.get_options()
  check_and_log_response(
      MesosClientAPI(options.cluster, options.verbosity).stage_recovery(backup_id))


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_print_recovery_tasks():
  """usage: scheduler_print_recovery_tasks --cluster=CLUSTER

  Prints all active tasks in a staged recovery.
  """
  options = app.get_options()
  resp = MesosClientAPI(options.cluster, options.verbosity).query_recovery(
      TaskQuery(statuses=ACTIVE_STATES))
  check_and_log_response(resp)
  log.info('Role\tJob\tShard\tStatus\tTask ID')
  for task in resp.tasks:
    assigned = task.assignedTask
    conf = assigned.task
    log.info('\t'.join((conf.owner.role,
                        conf.jobName,
                        str(conf.shardId),
                        ScheduleStatus._VALUES_TO_NAMES[task.status],
                        assigned.taskId)))


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('task_ids')
def scheduler_delete_recovery_tasks(task_ids):
  """usage: scheduler_delete_recovery_tasks --cluster=CLUSTER task_ids

  Deletes a comma-separated list of task IDs from a staged recovery.
  """
  ids = set(task_ids.split(','))
  options = app.get_options()
  check_and_log_response(MesosClientAPI(options.cluster, options.verbosity)
      .delete_recovery_tasks(TaskQuery(taskIds=ids)))


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_commit_recovery():
  """usage: scheduler_commit_recovery --cluster=CLUSTER

  Commits a staged recovery.
  """
  options = app.get_options()
  check_and_log_response(MesosClientAPI(options.cluster, options.verbosity)
      .commit_recovery())


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_unload_recovery():
  """usage: scheduler_unload_recovery --cluster=CLUSTER

  Unloads a staged recovery.
  """
  options = app.get_options()
  check_and_log_response(MesosClientAPI(options.cluster, options.verbosity)
      .unload_recovery())


def make_commands_str(commands):
  commands.sort()
  if len(commands) == 1:
    return str(commands[0])
  elif len(commands) == 2:
    return '%s (or %s)' % (str(commands[0]), str(commands[1]))
  else:
    return '%s (or any of: %s)' % (str(commands[0]), ' '.join(map(str, commands[1:])))


def generate_full_usage():
  docs_to_commands = collections.defaultdict(list)
  for (command, doc) in app.get_commands_and_docstrings():
    docs_to_commands[doc].append(command)
  def make_docstring(item):
    (doc_text, commands) = item
    def format_line(line):
      return '    %s\n' % line.lstrip()
    stripped = ''.join(map(format_line, doc_text.splitlines()))
    return '%s\n%s' % (make_commands_str(commands), stripped)
  usage = sorted(map(make_docstring, docs_to_commands.items()))
  return 'Available commands:\n\n' + '\n'.join(usage)


def generate_terse_usage():
  docs_to_commands = collections.defaultdict(list)
  for (command, doc) in app.get_commands_and_docstrings():
    docs_to_commands[doc].append(command)
  usage = '\n    '.join(sorted(map(make_commands_str, docs_to_commands.values())))
  return """
Available commands:
    %s

For more help on an individual command:
    %s help <command>
""" % (usage, app.name())


@app.command
def help(args):
  """usage: help [subcommand]

  Displays help for using mesos admin, or a specific subcommand.
  """
  if not args:
    print generate_full_usage()
    sys.exit(0)

  if len(args) > 1:
    die('Please specify at most one subcommand.')

  subcmd = args[0]
  if subcmd in globals():
    app.command_parser(subcmd).print_help()
  else:
    print 'Subcommand %s not found.' % subcmd
    sys.exit(1)


def set_quiet(option, _1, _2, parser):
  setattr(parser.values, option.dest, 'quiet')
  LogOptions.set_stderr_log_level('NONE')


def set_verbose(option, _1, _2, parser):
  setattr(parser.values, option.dest, 'verbose')
  LogOptions.set_stderr_log_level('DEBUG')


def main():
  app.help()


if __name__ == '__main__':
  # TODO(Sathya Hariesh): Possibly merge this with tools:mesosadm.
  app.interspersed_args(True)
  app.add_option('-v',
                 dest='verbosity',
                 default='normal',
                 action='callback',
                 callback=set_verbose,
                 help='Verbose logging. (default: %default)')
  app.add_option('-q',
                 dest='verbosity',
                 default='normal',
                 action='callback',
                 callback=set_quiet,
                 help='Quiet logging. (default: %default)')
  LogOptions.set_stderr_log_level('INFO')
  LogOptions.disable_disk_logging()
  app.set_name('mesos-admin')
  app.set_usage(DEFAULT_USAGE_BANNER + generate_terse_usage())
  app.main()

