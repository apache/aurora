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

import os
from contextlib import contextmanager

import mock
from twitter.common.contextutil import temporary_dir

from apache.thermos.cli.common import get_task_from_options

TASK_CONFIG = """
proc = Process(name = 'process', cmdline = 'echo hello world')
task = Task(name = 'task', processes = [proc])
export(task)
"""

MULTI_TASK_CONFIG = """
proc = Process(name = 'process', cmdline = 'echo hello world')
task = Task(name = 'task', processes = [proc])
export(task(name = 'task1'))
export(task(name = 'task2'))
"""


@contextmanager
def three_configs():
  with temporary_dir() as td:
    config_path = os.path.join(td, 'config.thermos')
    with open(config_path, 'w') as fp:
      fp.write(TASK_CONFIG)

    multi_path = os.path.join(td, 'multi_config.thermos')
    with open(multi_path, 'w') as fp:
      fp.write(MULTI_TASK_CONFIG)

    empty_path = os.path.join(td, 'empty_config.thermos')
    with open(empty_path, 'w') as fp:
      pass

    yield config_path, multi_path, empty_path


def get_options_mock(json=False, bindings={}, task=None):
  options_mock = mock.Mock(spec_set=('json', 'bindings', 'task'))
  options_mock.json = json
  options_mock.bindings = bindings
  options_mock.task = task
  return options_mock


def test_get_task_from_options():
  options_mock = get_options_mock()

  with three_configs() as (config_path, multi_path, _):
    # from single file w/o .task
    task = get_task_from_options([config_path], options_mock)
    assert str(task.task.name()) == 'task'

    # from .json
    json_path = config_path + '.json'
    task.to_file(json_path)
    options_mock.task = None
    options_mock.json = True
    task = get_task_from_options([json_path], options_mock)
    assert str(task.task.name()) == 'task'
    options_mock.json = False

    # from multi file
    options_mock.task = 'task1'
    task = get_task_from_options([multi_path], options_mock)
    assert str(task.task.name()) == 'task1'

    options_mock.task = 'task2'
    task = get_task_from_options([multi_path], options_mock)
    assert str(task.task.name()) == 'task2'


class ErrorException(Exception):
  pass


def raise_exception(msg):
  raise ErrorException(msg)


def catch_exception(stmt):
  try:
    stmt()
  except ErrorException as e:
    return e.args[0]


@mock.patch('apache.thermos.cli.common.app.error')
def test_get_task_from_options_invalid(error_mock):
  error_mock.side_effect = raise_exception
  options_mock = get_options_mock()

  with three_configs() as (config_path, multi_path, empty_path):
    exc = catch_exception(lambda: get_task_from_options([], options_mock))
    assert exc is not None
    assert exc.startswith('Should specify precisely one config')

    exc = catch_exception(lambda: get_task_from_options([config_path, config_path], options_mock))
    assert exc is not None
    assert exc.startswith('Should specify precisely one config')

    # unknown task name
    options_mock.task = 'unknown'
    exc = catch_exception(lambda: get_task_from_options([config_path], options_mock))
    assert exc is not None
    assert exc.startswith('Could not find task')

    # multiple tasks but not disambiguated
    options_mock.task = None
    exc = catch_exception(lambda: get_task_from_options([multi_path], options_mock))
    assert exc is not None
    assert exc.startswith('Multiple tasks in config but no task name specified')

    # no tasks defined
    exc = catch_exception(lambda: get_task_from_options([empty_path], options_mock))
    assert exc is not None
    assert exc.startswith('No tasks specified')
