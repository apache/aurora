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

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import EXIT_INVALID_CONFIGURATION, EXIT_INVALID_PARAMETER
from apache.aurora.client.cli.client import AuroraCommandLine

from .util import AuroraClientCommandTest

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    JobConfiguration,
    JobKey,
    PopulateJobResult,
    ResponseCode,
    Result,
    ScheduleStatusResult,
    TaskQuery
)


class TestDiffCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.env = None
    mock_options.json = False
    mock_options.bindings = {}
    mock_options.open_browser = False
    mock_options.rename_from = None
    mock_options.cluster = None
    return mock_options

  @classmethod
  def create_status_response(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(
        scheduleStatusResult=ScheduleStatusResult(tasks=set(cls.create_scheduled_tasks())))
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  @classmethod
  def setup_populate_job_config(cls, api):
    populate = cls.create_simple_success_response()
    api.populateJobConfig.return_value = populate
    populate.result = Result(populateJobResult=PopulateJobResult(
        taskConfig=cls.create_scheduled_tasks()[0].assignedTask.task))
    return populate

  def test_successful_diff(self):
    """Test the diff command."""
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (_, subprocess_patch, _):

      mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
      self.setup_populate_job_config(mock_scheduler_proxy)
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'diff', 'west/bozo/test/hello', fp.name])

        # Diff should get the task status, populate a config, and run diff.
        mock_scheduler_proxy.getTasksStatus.assert_called_with(
            TaskQuery(jobKeys=[JobKey(role='bozo', environment='test', name='hello')],
                      statuses=ACTIVE_STATES))
        assert mock_scheduler_proxy.populateJobConfig.call_count == 1
        assert isinstance(mock_scheduler_proxy.populateJobConfig.call_args[0][0], JobConfiguration)
        assert (mock_scheduler_proxy.populateJobConfig.call_args[0][0].key ==
            JobKey(environment=u'test', role=u'bozo', name=u'hello'))
        # Subprocess should have been used to invoke diff with two parameters.
        assert subprocess_patch.call_count == 1
        assert len(subprocess_patch.call_args[0][0]) == 3
        assert subprocess_patch.call_args[0][0][0] == os.environ.get('DIFF_VIEWER', 'diff')

  def test_diff_invalid_config(self):
    """Test the diff command if the user passes a config with an error in it."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    self.setup_populate_job_config(mock_scheduler_proxy)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (
            mock_scheduler_proxy_class,
            options,
            subprocess_patch,
            json_patch):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('stupid="me"',))
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'diff', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_INVALID_CONFIGURATION
        assert mock_scheduler_proxy.getTasksStatus.call_count == 0
        assert mock_scheduler_proxy.populateJobConfig.call_count == 0
        assert subprocess_patch.call_count == 0

  def test_diff_server_error(self):
    """Test the diff command if the user passes a config with an error in it."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_failed_status_response()
    self.setup_populate_job_config(mock_scheduler_proxy)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (
            mock_scheduler_proxy_class,
            options,
            subprocess_patch,
            json_patch):

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'diff', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_INVALID_PARAMETER
        # In this error case, we should have called the server getTasksStatus;
        # but since it fails, we shouldn't call populateJobConfig or subprocess.
        mock_scheduler_proxy.getTasksStatus.assert_called_with(
            TaskQuery(jobKeys=[JobKey(role='bozo', environment='test', name='hello')],
                statuses=ACTIVE_STATES))
        assert mock_scheduler_proxy.populateJobConfig.call_count == 0
        assert subprocess_patch.call_count == 0
