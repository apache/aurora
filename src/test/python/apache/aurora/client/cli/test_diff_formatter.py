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

import contextlib
import os
import textwrap

import pytest
from mock import Mock, call, patch
from pystachio import Empty

from apache.aurora.client.cli import Context
from apache.aurora.client.cli.diff_formatter import DiffFormatter
from apache.aurora.client.cli.jobs import DiffCommand
from apache.aurora.client.cli.options import TaskInstanceKey
from apache.aurora.config import AuroraConfig
from apache.aurora.config.schema.base import Job
from apache.thermos.config.schema_base import MB, Process, Resources, Task

from .util import AuroraClientCommandTest, FakeAuroraCommandContext, mock_verb_options

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    ConfigGroup,
    GetJobUpdateDiffResult,
    Range,
    Result,
    TaskQuery
)


class TestDiffFormatter(AuroraClientCommandTest):
  def setUp(self):
    self._fake_context = FakeAuroraCommandContext()
    self._mock_options = mock_verb_options(DiffCommand())
    self._mock_options.instance_spec = TaskInstanceKey(self.TEST_JOBKEY, [0, 1])
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api("west")

  @classmethod
  def get_job_config(self, is_cron=False):
    return AuroraConfig(job=Job(
      cluster='west',
      role='bozo',
      environment='test',
      name='the_job',
      service=True if not is_cron else False,
      cron_schedule='* * * * *' if is_cron else Empty,
      task=Task(
        name='task',
        processes=[Process(cmdline='ls -la', name='process')],
        resources=Resources(cpu=1.0, ram=1024 * MB, disk=1024 * MB)
      ),
      instances=3,
    ))

  @classmethod
  def get_job_update_diff_result(cls):
    diff = cls.create_simple_success_response()
    task = cls.create_task_config('foo')
    diff.result = Result(getJobUpdateDiffResult=GetJobUpdateDiffResult(
        add=set([ConfigGroup(
            config=task,
            instances=frozenset([Range(first=10, last=10), Range(first=12, last=14)]))]),
        remove=frozenset(),
        update=frozenset([ConfigGroup(
            config=task,
            instances=frozenset([Range(first=11, last=11)]))]),
        unchanged=frozenset([ConfigGroup(
            config=task,
            instances=frozenset([Range(first=0, last=9)]))])
    ))
    return diff

  @classmethod
  def get_job_update_no_change_diff_result(cls):
    diff = cls.create_simple_success_response()
    task = cls.create_task_config('foo')
    diff.result = Result(getJobUpdateDiffResult=GetJobUpdateDiffResult(
        add=frozenset(),
        remove=frozenset(),
        update=frozenset(),
        unchanged=frozenset([ConfigGroup(
            config=task,
            instances=frozenset([Range(first=0, last=3)]))])
    ))
    return diff

  def test_show_job_update_diff_with_task_diff(self):
    config = self.get_job_config()
    self._fake_context.get_job_config = Mock(return_value=config)
    formatter = DiffFormatter(self._fake_context, config)
    local_task = self.create_scheduled_tasks()[0].assignedTask.task
    self._mock_api.get_job_update_diff.return_value = self.get_job_update_diff_result()

    with contextlib.nested(
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value={})) as (subprocess_patch, _):

      formatter.show_job_update_diff(self._mock_options.instance_spec.instance, local_task)

      assert self._mock_api.get_job_update_diff.mock_calls == [
          call(config, self._mock_options.instance_spec.instance)
      ]
      assert "\n".join(self._fake_context.get_out()) == textwrap.dedent("""\
        This job update will:
        add instances: [10], [12-14]
        update instances: [11]
        with diff:\n\n
        not change instances: [0-9]""")
      assert subprocess_patch.call_count == 1
      assert subprocess_patch.call_args[0][0].startswith(
          os.environ.get('DIFF_VIEWER', 'diff') + ' ')

  def test_show_job_update_diff_without_task_diff(self):
    config = self.get_job_config()
    self._fake_context.get_job_config = Mock(return_value=config)
    formatter = DiffFormatter(self._fake_context, config)
    self._mock_api.get_job_update_diff.return_value = self.get_job_update_diff_result()

    formatter.show_job_update_diff(self._mock_options.instance_spec.instance)

    assert self._mock_api.get_job_update_diff.mock_calls == [
        call(config, self._mock_options.instance_spec.instance)
    ]
    assert "\n".join(self._fake_context.get_out()) == textwrap.dedent("""\
      This job update will:
      add instances: [10], [12-14]
      update instances: [11]
      not change instances: [0-9]""")

  def test_show_job_update_diff_no_change(self):
    config = self.get_job_config()
    self._fake_context.get_job_config = Mock(return_value=config)
    formatter = DiffFormatter(self._fake_context, config)
    self._mock_api.get_job_update_diff.return_value = self.get_job_update_no_change_diff_result()

    formatter.show_job_update_diff(self._mock_options.instance_spec.instance)

    assert self._mock_api.get_job_update_diff.mock_calls == [
        call(config, self._mock_options.instance_spec.instance)
    ]
    assert "\n".join(self._fake_context.get_out()) == textwrap.dedent("""\
      This job update will:
      not change instances: [0-3]""")

  def test_get_job_update_diff_error(self):
    mock_config = self.get_job_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)
    formatter = DiffFormatter(self._fake_context, mock_config)
    self._mock_api.get_job_update_diff.return_value = self.create_error_response()

    with pytest.raises(Context.CommandError):
      formatter.show_job_update_diff(self._mock_options.instance_spec.instance)

    assert self._mock_api.get_job_update_diff.mock_calls == [
      call(mock_config, self._mock_options.instance_spec.instance)
    ]
    assert self._fake_context.get_out() == []
    assert self._fake_context.get_err() == ["Error getting diff info from scheduler", "\tWhoops"]

  def test_diff_no_update_details_success(self):
    config = self.get_job_config(is_cron=True)
    self._fake_context.get_job_config = Mock(return_value=config)
    formatter = DiffFormatter(self._fake_context, config)
    self._mock_api.query.return_value = self.create_empty_task_result()
    query = TaskQuery(
      jobKeys=[self.TEST_JOBKEY.to_thrift()],
      statuses=ACTIVE_STATES)
    self._mock_api.build_query.return_value = query
    local_tasks = []

    with contextlib.nested(
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value={})) as (subprocess_patch, _):

      formatter.diff_no_update_details(local_tasks)

      assert subprocess_patch.call_count == 1
      assert subprocess_patch.call_args[0][0].startswith(
          os.environ.get('DIFF_VIEWER', 'diff') + ' ')
