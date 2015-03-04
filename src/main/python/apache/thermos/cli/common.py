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

import os
import re
import sys

from twitter.common import app, log

from apache.thermos.config.loader import ThermosConfigLoader
from apache.thermos.core.runner import TaskRunner
from apache.thermos.monitoring.detector import ChainedPathDetector, TaskDetector


def get_task_from_options(args, opts, **kw):
  loader = ThermosConfigLoader.load_json if opts.json else ThermosConfigLoader.load

  if len(args) != 1:
    app.error('Should specify precisely one config, instead got: %s' % args)

  tasks = loader(args[0], bindings=opts.bindings, **kw)

  task_list = list(tasks.tasks())
  if len(task_list) == 0:
    app.error("No tasks specified!")

  if opts.task is None and len(task_list) > 1:
    app.error("Multiple tasks in config but no task name specified!")

  task = None
  if opts.task is not None:
    for t in task_list:
      if t.task().name().get() == opts.task:
        task = t
        break
    if task is None:
      app.error("Could not find task %s!" % opts.task)
  else:
    task = task_list[0]

  return task


def daemonize():
  def daemon_fork():
    try:
      if os.fork() > 0:
        os._exit(0)
    except OSError as e:
      sys.stderr.write('[pid:%s] Failed to fork: %s\n' % (os.getpid(), e))
      sys.exit(1)
  daemon_fork()
  os.setsid()
  daemon_fork()
  sys.stdin, sys.stdout, sys.stderr = (open('/dev/null', 'r'),  # noqa
                                       open('/dev/null', 'a+'),  # noqa
                                       open('/dev/null', 'a+', 0))  # noqa


def really_run(
    task,
    root,
    sandbox,
    task_id=None,
    user=None,
    prebound_ports=None,
    chroot=None,
    daemon=False):

  prebound_ports = prebound_ports or {}
  missing_ports = set(task.ports()) - set(prebound_ports.keys())
  if missing_ports:
    app.error('ERROR!  Unbound ports: %s' % ' '.join(port for port in missing_ports))
  task_runner = TaskRunner(task.task, root, sandbox, task_id=task_id,
                           user=user, portmap=prebound_ports, chroot=chroot)
  if daemon:
    print('Daemonizing and starting runner.')
    try:
      log.teardown_stderr_logging()
      daemonize()
    except Exception as e:
      print("Failed to daemonize: %s" % e)
      sys.exit(1)
  try:
    task_runner.run()
  except KeyboardInterrupt:
    print('Got keyboard interrupt, killing job!')
    task_runner.close_ckpt()
    task_runner.kill()


def generate_usage():
  usage = """
thermos

commands:
"""

  for (command, doc) in app.get_commands_and_docstrings():
    usage += '    ' + '%-10s' % command + '\t' + doc.split('\n')[0].strip() + '\n'
  app.set_usage(usage)


__PATH_DETECTORS = [
]


def clear_path_detectors():
  __PATH_DETECTORS[:] = []


def register_path_detector(path_detector):
  __PATH_DETECTORS.append(path_detector)


def get_path_detector():
  return ChainedPathDetector(*__PATH_DETECTORS)


def tasks_from_re(expressions, state=None):
  path_detector = get_path_detector()

  matched_tasks = set()

  for root in path_detector.get_paths():
    task_ids = [t_id for _, t_id in TaskDetector(root).get_task_ids(state=state)]
    for task_expr in map(re.compile, expressions):
      for task_id in task_ids:
        if task_expr.match(task_id):
          matched_tasks.add((root, task_id))

  return matched_tasks
