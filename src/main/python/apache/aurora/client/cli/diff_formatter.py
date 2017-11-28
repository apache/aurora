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

import json
import os
import pprint
import subprocess
from copy import deepcopy
from itertools import chain
from pipes import quote
from tempfile import NamedTemporaryFile

from apache.aurora.client.cli import EXIT_COMMAND_FAILURE, EXIT_INVALID_PARAMETER

from gen.apache.aurora.api.constants import ACTIVE_STATES, AURORA_EXECUTOR_NAME
from gen.apache.aurora.api.ttypes import ExecutorConfig


class DiffFormatter(object):
  def __init__(self, context, config, cluster=None, role=None, env=None, name=None):
    self.context = context
    self.config = config
    self.cluster = config.cluster() if cluster is None else cluster
    self.role = config.role() if role is None else role
    self.env = config.environment() if env is None else env
    self.name = config.name() if name is None else name
    self.prettyprinter = pprint.PrettyPrinter(indent=2)

  def _pretty_print_task(self, task):
    task.configuration = None
    executor_config = json.loads(task.executorConfig.data)
    pretty_executor = json.dumps(executor_config, indent=2, sort_keys=True)
    pretty_executor = '\n'.join(["    %s" % s for s in pretty_executor.split("\n")])
    # Make start cleaner, and display multi-line commands across multiple lines.
    pretty_executor = '\n    ' + pretty_executor.replace(r'\n', '\n')

    # Avoid re-escaping as it's already pretty printed.
    class RawRepr(object):
      def __init__(self, data):
        self.data = data

      def __repr__(self):
        return self.data

    # Sorting sets, hence they turn into lists
    if task.constraints:
      task.constraints = sorted(task.constraints, key=str)

    if task.metadata:
      task.metadata = sorted(task.metadata, key=str)

    if task.resources:
      task.resources = sorted(task.resources, key=str)

    if task.mesosFetcherUris:
      task.mesosFetcherUris = sorted(task.mesosFetcherUris, key=str)

    task.executorConfig = ExecutorConfig(
      name=AURORA_EXECUTOR_NAME,
      data=RawRepr(pretty_executor))
    return self.prettyprinter.pformat(vars(task))

  def _pretty_print_tasks(self, tasks):
    return ",\n".join(self._pretty_print_task(t) for t in tasks)

  def _dump_tasks(self, tasks, out_file):
    out_file.write(self._pretty_print_tasks(tasks))
    out_file.write("\n")
    out_file.flush()

  def _diff_tasks(self, local_tasks, remote_tasks):
    diff_program = os.environ.get("DIFF_VIEWER", "diff")
    with NamedTemporaryFile() as local:
      self._dump_tasks(local_tasks, local)
      with NamedTemporaryFile() as remote:
        self._dump_tasks(remote_tasks, remote)
        process = subprocess.Popen(
          [diff_program, quote(remote.name), quote(local.name)],
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        # Unlike most commands, diff doesn't return zero on success; it returns
        # 1 when a successful diff is non-empty.
        if process.poll() not in (0, 1):
          raise self.context.CommandError(EXIT_COMMAND_FAILURE, "Error running diff: %s" % stderr)
        self.context.print_out(stdout)

  def _show_diff(self, header, configs_summaries, local_task=None):
    def min_start(ranges):
      return min(ranges, key=lambda r: r.first).first

    def format_ranges(ranges):
      instances = []
      for task_range in sorted(list(ranges), key=lambda r: r.first):
        if task_range.first == task_range.last:
          instances.append("[%s]" % task_range.first)
        else:
          instances.append("[%s-%s]" % (task_range.first, task_range.last))
      return instances

    def print_instances(instances):
      self.context.print_out("%s %s" % (header, ", ".join(str(span) for span in instances)))

    summaries = sorted(list(configs_summaries), key=lambda s: min_start(s.instances))

    if local_task:
      for summary in summaries:
        print_instances(format_ranges(summary.instances))
        self.context.print_out("with diff:\n")
        self._diff_tasks([deepcopy(local_task)], [summary.config])
        self.context.print_out('')
    else:
      if summaries:
        print_instances(
          format_ranges(r for r in chain.from_iterable(s.instances for s in summaries)))

  def diff_no_update_details(self, local_tasks):
    # Deepcopy is important here as tasks will be modified for printing.
    local_tasks = [deepcopy(t) for t in local_tasks]
    api = self.context.get_api(self.cluster)
    resp = api.query(api.build_query(self.role, self.name, env=self.env, statuses=ACTIVE_STATES))
    self.context.log_response_and_raise(
      resp,
      err_code=EXIT_INVALID_PARAMETER,
      err_msg="Could not find job to diff against")
    if resp.result.scheduleStatusResult.tasks is None:
      self.context.print_err("No tasks found for job %s" %
                             self.context.options.instance_spec.jobkey)
      return EXIT_COMMAND_FAILURE
    else:
      remote_tasks = [t.assignedTask.task for t in resp.result.scheduleStatusResult.tasks]
      self._diff_tasks(local_tasks, remote_tasks)

  def show_job_update_diff(self, instances, local_task=None):
    api = self.context.get_api(self.cluster)
    resp = api.get_job_update_diff(self.config, instances)
    self.context.log_response_and_raise(
      resp,
      err_code=EXIT_COMMAND_FAILURE,
      err_msg="Error getting diff info from scheduler")
    diff = resp.result.getJobUpdateDiffResult
    self.context.print_out("This job update will:")
    self._show_diff("add instances:", diff.add)
    self._show_diff("remove instances:", diff.remove)
    self._show_diff("remove instances:", diff.remove)
    self._show_diff("update instances:", diff.update, local_task)
    self._show_diff("not change instances:", diff.unchanged)
