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
import json
import os

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import EXIT_INVALID_CONFIGURATION, EXIT_INVALID_PARAMETER, EXIT_OK
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    ExecutorConfig,
    Identity,
    JobConfiguration,
    JobKey,
    Metadata,
    PopulateJobResult,
    Response,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery
)

MOCK_LOG = []


def mock_log(*args):
  MOCK_LOG.append(args)


def clear_mock_log():
  global MOCK_LOG
  MOCK_LOG = []


MOCK_OUT = []


def mock_out(s):
  MOCK_OUT.append(s)


def clear_mock_out():
  global MOCK_OUT
  MOCK_OUT = []


class TestDiffCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options = Mock()
    mock_options.env = None
    mock_options.json = False
    mock_options.bindings = {}
    mock_options.open_browser = False
    mock_options.rename_from = None
    mock_options.cluster = None
    return mock_options

  @classmethod
  def create_mock_scheduled_task(cls, task_name, max_failures, num_cpus, role, metadata):
    task = ScheduledTask()
    task.key = JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=task_name)
    task.failure_count = 0
    task.assignedTask = Mock(spec=AssignedTask)
    task.assignedTask.slaveHost = 'slavehost'
    task.assignedTask.task = TaskConfig()
    task.assignedTask.task.maxTaskFailures = max_failures
    task.assignedTask.task.executorConfig = ExecutorConfig()
    task.assignedTask.task.executorConfig.data = '[]'
    task.assignedTask.task.metadata = metadata
    task.assignedTask.task.owner = Identity(role=role)
    task.assignedTask.task.environment = 'test'
    task.assignedTask.task.jobName = task_name
    task.assignedTask.task.numCpus = num_cpus
    task.assignedTask.task.ramMb = 2
    task.assignedTask.task.diskMb = 2
    task.assignedTask.instanceId = 4237894
    task.assignedTask.assignedPorts = None
    task.status = ScheduleStatus.RUNNING
    mockEvent = Mock(spec=TaskEvent)
    mockEvent.timestamp = 28234726395
    mockEvent.status = ScheduleStatus.RUNNING
    mockEvent.message = "Hi there"
    task.taskEvents = [mockEvent]
    return task

  @classmethod
  def create_mock_scheduled_tasks(cls, task_specs=None):
    tasks = []
    if task_specs is None:
      task_specs = [{'name': 'foo'}, {'name': 'bar'}, {'name': 'baz'}]
    for task_spec in task_specs:
      task = cls.create_mock_scheduled_task(task_spec["name"],
          task_spec.get("max_failures", 1),
          num_cpus=task_spec.get("num_cpus", 2),
          role=task_spec.get("role", "bozo"),
          metadata=task_spec.get("metadata", []))
      tasks.append(task)
    return tasks

  @classmethod
  def create_mock_taskconfigs(cls, task_specs=None):
    tasks = []
    if task_specs is None:
      task_specs = [{'name': 'foo'}, {'name': 'bar'}, {'name': 'baz'}]
    for task_spec in task_specs:
      task = TaskConfig()
      task.maxTaskFailures = task_spec.get("max_task_failures", 1)
      task.executorConfig = ExecutorConfig()
      task.executorConfig.data = '[]'
      task.metadata = task_spec.get("metadata", [])
      task.owner = Identity(role=task_spec.get("role", "bozo"))
      task.environment = 'test'
      task.jobName = task_spec['name']
      task.numCpus = task_spec.get("num_cpus", 2)
      task.ramMb = 2
      task.diskMb = 2
      tasks.append(task)
    return tasks

  @classmethod
  def create_status_response(cls, specs=None):
    resp = cls.create_simple_success_response()
    resp.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    resp.result.scheduleStatusResult.tasks = set(cls.create_mock_scheduled_tasks(specs))
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  @classmethod
  def setup_populate_job_config(cls, api, task_specs=None):
    populate = Response()
    populate.responseCode = ResponseCode.OK
    populate.messageDEPRECATED = "Ok"
    populate.result = Result()
    populate.result.populateJobResult = PopulateJobResult()
    populate.result.populateJobResult.populated = cls.create_mock_taskconfigs(task_specs)
    api.populateJobConfig.return_value = populate
    return populate

  def test_success_no_diffs(self):
    result = self._test_successful_diff_generic(None, None)
    assert MOCK_OUT == ["No diffs found!"]
    assert result == EXIT_OK

  def test_success_no_diffs_metadata(self):
    # Metadata in different order, but same data.
    one = [{"name": "serv", "metadata": [Metadata(key="a", value="1"),
        Metadata(key="b", value="2"), Metadata(key="instance", value="0")]},
        {"name": "serv", "metadata": [Metadata(key="a", value="1"),
            Metadata(key="b", value="2"), Metadata(key="instance", value="1")]},
        {"name": "serv", "metadata": [Metadata(key="a", value="1"),
            Metadata(key="b", value="2"), Metadata(key="instance", value="2")]}]

    two = [{"name": "serv", "metadata": [Metadata(key="b", value="2"),
        Metadata(key="a", value="1"), Metadata(key="instance", value="0")]},
        {"name": "serv", "metadata": [Metadata(key="instance", value="1"),
        Metadata(key="a", value="1"), Metadata(key="b", value="2")]},
        {"name": "serv", "metadata": [Metadata(key="a", value="1"),
        Metadata(key="instance", value="2"), Metadata(key="b", value="2")]}]

    result = self._test_successful_diff_generic(one, two)
    assert result == EXIT_OK
    assert MOCK_OUT == ["No diffs found!"]

  def test_success_diffs_metadata(self):
    one = [{"name": "serv", "metadata": [Metadata(key="a", value="1"),
        Metadata(key="b", value="2"), Metadata(key="instance", value="0")]},
        {"name": "serv", "metadata": [Metadata(key="a", value="1"),
            Metadata(key="b", value="2"), Metadata(key="instance", value="1")]},
        {"name": "serv", "metadata": [Metadata(key="a", value="1"),
            Metadata(key="b", value="2"), Metadata(key="instance", value="2")]}]

    two = [{"name": "serv", "metadata": [Metadata(key="b", value="2"),
        Metadata(key="a", value="1"), Metadata(key="instance", value="0")]},
        {"name": "serv", "metadata": [Metadata(key="instance", value="1"),
        Metadata(key="a", value="3"), Metadata(key="b", value="2")]},
        {"name": "serv", "metadata": [Metadata(key="a", value="1"),
        Metadata(key="instance", value="2"), Metadata(key="b", value="2")]}]

    result = self._test_successful_diff_generic(one, two)
    assert result == EXIT_OK
    print(MOCK_OUT)
    assert MOCK_OUT == ['Task diffs found in instance 1',
        (u"\tField 'metadata' is '[{u'key': u'a', u'value': u'3'}, {u'key': u'b', u'value': u'2'}, "
             "{u'key': u'instance', u'value': u'1'}]' local, but '[{u'key': u'a', u'value': u'1'}, "
            "{u'key': u'b', u'value': u'2'}, {u'key': u'instance', u'value': u'1'}]' remote"),
        '1 total diff(s) found']

  def test_success_no_diffs_json(self):
    result = self._test_successful_diff_generic(None, None, write_json=True)
    # No diffs, in json, shows as an empty list of diffs
    assert MOCK_OUT == ["[]"]
    assert result == EXIT_OK

  def test_success_with_diffs_one(self):
    # owner.role different in task 0
    result = self._test_successful_diff_generic([{"name": "foo", "role": "me"},
            {"name": "bar", "role": "you"}, {"name": "baz", "role": "you"}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you"},
            {"name": "baz", "role": "you"}])
    assert result == EXIT_OK
    assert MOCK_OUT == ["Task diffs found in instance 0",
        "\tField 'owner.role' is 'you' local, but 'me' remote",
        "1 total diff(s) found"]

  def test_success_with_diffs_one_exclude_owner_role(self):
    # owner.role different in task 0
    result = self._test_successful_diff_generic([{"name": "foo", "role": "me"},
            {"name": "bar", "role": "you"}, {"name": "baz", "role": "you"}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you"},
            {"name": "baz", "role": "you"}],
        excludes=["owner.role"])
    assert result == EXIT_OK
    assert MOCK_OUT == ["No diffs found!"]

  def test_success_with_diffs_one_json(self):
    # owner.role different in task 0
    result = self._test_successful_diff_generic([{"name": "foo", "role": "me"},
            {"name": "bar", "role": "you"}, {"name": "baz", "role": "you"}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you"},
            {"name": "baz", "role": "you"}], write_json=True)
    assert result == EXIT_OK
    out_json = json.loads(''.join(MOCK_OUT))
    assert len(out_json) == 1
    assert out_json[0]["task"] == 0
    assert out_json[0]["difftype"] == "fields"
    assert len(out_json[0]["fields"]) == 1
    assert out_json[0]["fields"][0]["field"] == "owner.role"
    assert out_json[0]["fields"][0]["local"] == "you"
    assert out_json[0]["fields"][0]["remote"] == "me"

  def test_success_with_diffs_two(self):
    # local has more tasks than remote
    result = self._test_successful_diff_generic([{"name": "foo", "role": "you"},
            {"name": "bar", "role": "you"}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you"},
            {"name": "baz", "role": "you"}])
    assert result == EXIT_OK
    assert MOCK_OUT == ["Local config has a different number of tasks: 3 local vs 2 running",
        "1 total diff(s) found"]

  def test_success_with_diffs_two_and_a_half(self):
    # Reverse of test two
    result = self._test_successful_diff_generic([{"name": "foo", "role": "you"},
            {"name": "bar", "role": "you"}, {"name": "baz", "role": "you"}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you"}])
    assert result == EXIT_OK
    assert MOCK_OUT == ["Local config has a different number of tasks: 2 local vs 3 running",
        "1 total diff(s) found"]

  def test_success_with_diffs_two_json(self):
    # local has more tasks than remote
    result = self._test_successful_diff_generic([{"name": "foo", "role": "you"},
            {"name": "bar", "role": "you"}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you"},
            {"name": "baz", "role": "you"}], write_json=True)
    assert result == EXIT_OK
    out_json = json.loads("".join(MOCK_OUT))
    assert len(out_json) == 1
    assert out_json[0]["difftype"] == "num_tasks"
    assert out_json[0]["local"] == 3
    assert out_json[0]["remote"] == 2

  def test_success_with_diffs_three(self):
    # local has more tasks than remote, and local task 1 has a different numCpus
    result = self._test_successful_diff_generic([{"name": "foo", "role": "you"},
            {"name": "bar", "role": "you"}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you", "num_cpus": 4},
            {"name": "baz", "role": "you"}])
    assert result == EXIT_OK
    assert MOCK_OUT == ["Local config has a different number of tasks: 3 local vs 2 running",
        "Task diffs found in instance 1",
        "\tField 'numCpus' is '4' local, but '2' remote",
        "2 total diff(s) found"]

  def test_success_with_diffs_three_json(self):
    # local has more tasks than remote, and local task 1 has a different numCpus
    result = self._test_successful_diff_generic([{"name": "foo", "role": "you"},
            {"name": "bar", "role": "you"}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you", "num_cpus": 4},
            {"name": "baz", "role": "you"}], write_json=True)
    assert result == EXIT_OK
    json_out = json.loads("".join(MOCK_OUT))
    assert len(json_out) == 2
    def matches_numtasks_diff(f):
      return f["difftype"] == "num_tasks" and f["remote"] == 2 and f["local"] == 3
    assert any(matches_numtasks_diff(entry) for entry in json_out)

    def matches_fields_diff(f):
      if f["difftype"] != "fields" or f["task"] != 1:
        return False
      if len(f["fields"]) != 1:
        return False
      field = f["fields"][0]
      return field["field"] == "numCpus" and field["local"] == 4 and field["remote"] == 2
    assert any(matches_fields_diff(entry) for entry in json_out)

  def test_success_with_diffs_four(self):
    # Same number of tasks, but task 0 has a different name, task 1 has a different role,
    # and task 3 has a different number of cpus.
    result = self._test_successful_diff_generic([{"name": "foobie", "role": "you"},
            {"name": "bar", "role": "him"}, {"name": "baz", "role": "you", "num_cpus": 3}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you"},
         {"name": "baz", "role": "you", "num_cpus": 4}])

    assert result == EXIT_OK
    assert MOCK_OUT == ["Task diffs found in instance 0",
       "\tField 'jobName' is 'foo' local, but 'foobie' remote",
       "Task diffs found in instance 1",
       "\tField 'owner.role' is 'you' local, but 'him' remote",
       "Task diffs found in instance 2",
       "\tField 'numCpus' is '4' local, but '3' remote",
       "3 total diff(s) found"]

  def test_success_with_diffs_four_exclude(self):
    # Same number of tasks, but task 0 has a different name, task 1 has a different role,
    # and task 3 has a different number of cpus.
    result = self._test_successful_diff_generic([{"name": "foobie", "role": "you"},
            {"name": "bar", "role": "him"}, {"name": "baz", "role": "you", "num_cpus": 3}],
        [{"name": "foo", "role": "you"}, {"name": "bar", "role": "you"},
         {"name": "baz", "role": "you", "num_cpus": 4}],
        excludes=["jobName", "owner.role"])

    assert result == EXIT_OK
    assert MOCK_OUT == ["Task diffs found in instance 2",
       "\tField 'numCpus' is '4' local, but '3' remote",
       "1 total diff(s) found"]

  def _test_successful_diff_generic(
      self,
      remote_task_spec,
      local_task_spec,
      write_json=False,
      excludes=None):

    """Generic version of json-tree diff test.
    :param remote_task_spec: a list of dictionaries, containing parameters used to fill in
        the task configs generated by the test for mock calls to getTaskStatus.
    :param local_task_spec: a list of dictionaries, containing parameters used to fill in
        the task configs generated by the test for mock calls to populateJobConfig.
    :param write_json: flag indicating whether the test should generate json output or
        user-friendly output.
    :param excludes: a list of fields that should be specified for exclusion in the test
       using --exclude-field parameters.

    For the task_spec parameters, the dictionaries can contain the following keys:
    - name: mandatory field containing the job name for the task.
    - role: the value of the "role" field.
    - num_cpus: the value of the numCpus field
    - max_task_failures: the value of the maxTaskFailures field.
    """
    clear_mock_log()
    clear_mock_out()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    def foo(*args, **kwargs):
      return mock_scheduler_proxy

    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.cli.print_aurora_log', side_effect=mock_log),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_out',
            side_effect=mock_out),
        patch('subprocess.call', return_value=0)):
      mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response(
          remote_task_spec)
      self.setup_populate_job_config(mock_scheduler_proxy, local_task_spec)
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        params = ['job', 'diff', 'west/bozo/test/hello', fp.name]
        if write_json:
          params.append("--write-json")
        if excludes is not None:
          for e in excludes:
            params.append("--exclude-field=%s" % e)
        result = cmd.execute(params)

        # Diff should get the task status, populate a config, and run diff.
        mock_scheduler_proxy.getTasksStatus.assert_called_with(
            TaskQuery(jobName='hello', environment='test', owner=Identity(role='bozo'),
                statuses=ACTIVE_STATES))
        assert mock_scheduler_proxy.populateJobConfig.call_count == 1
        assert isinstance(mock_scheduler_proxy.populateJobConfig.call_args[0][0], JobConfiguration)
        assert (mock_scheduler_proxy.populateJobConfig.call_args[0][0].key ==
            JobKey(environment=u'test', role=u'bozo', name=u'hello'))
        return result


  def test_diff_invalid_config(self):
    """Test the diff command if the user passes a config with an error in it."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    self.setup_populate_job_config(mock_scheduler_proxy)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (
            mock_scheduler_proxy_class,
            mock_clusters,
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
        return result

  def test_diff_server_error(self):
    """Test the diff command if the user passes a config with an error in it."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_failed_status_response()
    self.setup_populate_job_config(mock_scheduler_proxy)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (
            mock_scheduler_proxy_class,
            mock_clusters,
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
            TaskQuery(jobName='hello', environment='test', owner=Identity(role='bozo'),
                statuses=ACTIVE_STATES))
        assert mock_scheduler_proxy.populateJobConfig.call_count == 0
        assert subprocess_patch.call_count == 0

  def test_successful_unix_diff(self):
    """Test the old shell-based diff method."""
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (_, _, subprocess_patch, _):
      mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
      self.setup_populate_job_config(mock_scheduler_proxy)
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'diff', '--use-shell-diff', 'west/bozo/test/hello', fp.name])

        # Diff should get the task status, populate a config, and run diff.
        mock_scheduler_proxy.getTasksStatus.assert_called_with(
            TaskQuery(jobName='hello', environment='test', owner=Identity(role='bozo'),
                statuses=ACTIVE_STATES))
        assert mock_scheduler_proxy.populateJobConfig.call_count == 1
        assert isinstance(mock_scheduler_proxy.populateJobConfig.call_args[0][0], JobConfiguration)
        assert (mock_scheduler_proxy.populateJobConfig.call_args[0][0].key ==
            JobKey(environment=u'test', role=u'bozo', name=u'hello'))
        # Subprocess should have been used to invoke diff with two parameters.
        assert subprocess_patch.call_count == 1
        assert len(subprocess_patch.call_args[0][0]) == 3
        assert subprocess_patch.call_args[0][0][0] == os.environ.get('DIFF_VIEWER', 'diff')
