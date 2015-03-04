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

from twitter.common import app
from twitter.common.quantity.parse_simple import parse_data, parse_time

from apache.thermos.cli.common import get_path_detector, tasks_from_re
from apache.thermos.monitoring.garbage import GarbageCollectionPolicy, TaskGarbageCollector


def set_keep(option, opt_str, value, parser):
  setattr(parser.values, option.dest, opt_str.startswith('--keep'))


@app.command
@app.command_option("--max_age", metavar="AGE", default=None, dest='max_age',
                    help="Max age in human readable form, e.g. 2d5h or 7200s")
@app.command_option("--max_tasks", metavar="NUM", default=None, dest='max_tasks',
                    help="Max number of tasks to keep.")
@app.command_option("--max_space", metavar="SPACE", default=None, dest='max_space',
                    help="Max space to allow for tasks, e.g. 20G.")
@app.command_option("--keep-logs", "--delete-logs",
                    metavar="PATH", default=True,
                    action='callback', callback=set_keep, dest='keep_logs',
                    help="Keep logs.")
@app.command_option("--keep-metadata", "--delete-metadata",
                    metavar="PATH", default=True,
                    action='callback', callback=set_keep, dest='keep_metadata',
                    help="Keep metadata.")
@app.command_option("--force", default=False, action='store_true', dest='force',
                    help="Perform garbage collection without confirmation")
@app.command_option("--dryrun", default=False, action='store_true', dest='dryrun',
                    help="Don't actually run garbage collection.")
def gc(args, options):
  """Garbage collect task(s) and task metadata.

    Usage: thermos gc [options] [task_id1 task_id2 ...]

    If tasks specified, restrict garbage collection to only those tasks,
    otherwise all tasks are considered.  The optional constraints are still
    honored.
  """
  print('Analyzing root at %s' % options.root)
  gc_options = {}
  if options.max_age is not None:
    gc_options['max_age'] = parse_time(options.max_age)
  if options.max_space is not None:
    gc_options['max_space'] = parse_data(options.max_space)
  if options.max_tasks is not None:
    gc_options['max_tasks'] = int(options.max_tasks)
  gc_options.update(include_metadata=not options.keep_metadata,
                    include_logs=not options.keep_logs,
                    verbose=True,
                    logger=print)
  if args:
    gc_tasks = list(tasks_from_re(args, state='finished'))
  else:
    print('No task ids specified, using default collector.')
    gc_tasks = [(task.checkpoint_root, task.task_id)
        for task in GarbageCollectionPolicy(get_path_detector(), **gc_options).run()]

  if not gc_tasks:
    print('No tasks to garbage collect.  Exiting')
    return

  def maybe(function, *args):
    if options.dryrun:
      print('    would run %s%r' % (function.__name__, args))
    else:
      function(*args)

  value = 'y'
  if not options.force:
    value = raw_input("Continue [y/N]? ") or 'N'
  if value.lower() == 'y':
    print('Running gc...')

    for checkpoint_root, task_id in gc_tasks:
      tgc = TaskGarbageCollector(checkpoint_root, task_id)
      print('  Task %s ' % task_id, end='')
      print('data (%s) ' % ('keeping' if options.keep_data else 'deleting'), end='')
      print('logs (%s) ' % ('keeping' if options.keep_logs else 'deleting'), end='')
      print('metadata (%s) ' % ('keeping' if options.keep_metadata else 'deleting'))
      if not options.keep_data:
        maybe(tgc.erase_data)
      if not options.keep_logs:
        maybe(tgc.erase_logs)
      if not options.keep_metadata:
        maybe(tgc.erase_metadata)
      print('done.')
  else:
    print('Cancelling gc.')
