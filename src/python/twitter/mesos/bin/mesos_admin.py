"""Command-line client for managing admin-only interactions with the aurora scheduler.
"""

import collections
import optparse
import sys

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.mesos.client import client_util
from twitter.mesos.client.client_wrapper import MesosClientAPI
from twitter.mesos.client.client_util import requires

from gen.twitter.mesos.constants import ACTIVE_STATES
from gen.twitter.mesos.ttypes import (
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


def is_verbose():
  return app.get_options().verbosity == 'verbose'


def create_client(cluster=None):
  if cluster is None:
    cluster = app.get_options().cluster
  return MesosClientAPI(cluster=cluster, verbose=is_verbose())


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

  resp = create_client().set_quota(role, cpu, ram_mb, disk_mb)
  client_util.check_and_log_response(resp)


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

  resp = create_client().force_task_state(task_id, status)
  client_util.check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_backup_now():
  """usage: scheduler_backup_now --cluster=CLUSTER

  Immediately initiates a full storage backup.
  """
  client_util.check_and_log_response(create_client().perform_backup())


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_list_backups():
  """usage: scheduler_list_backups --cluster=CLUSTER

  Lists backups available for recovery.
  """
  resp = create_client().list_backups()
  client_util.check_and_log_response(resp)
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
  client_util.check_and_log_response(create_client().stage_recovery(backup_id))


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_print_recovery_tasks():
  """usage: scheduler_print_recovery_tasks --cluster=CLUSTER

  Prints all active tasks in a staged recovery.
  """
  resp = create_client().query_recovery(TaskQuery(statuses=ACTIVE_STATES))
  client_util.check_and_log_response(resp)
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
  client_util.check_and_log_response(create_client().delete_recovery_tasks(TaskQuery(taskIds=ids)))


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_commit_recovery():
  """usage: scheduler_commit_recovery --cluster=CLUSTER

  Commits a staged recovery.
  """
  client_util.check_and_log_response(create_client().commit_recovery())


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def scheduler_unload_recovery():
  """usage: scheduler_unload_recovery --cluster=CLUSTER

  Unloads a staged recovery.
  """
  client_util.check_and_log_response(create_client().unload_recovery())


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
    client_util.die('Please specify at most one subcommand.')

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

