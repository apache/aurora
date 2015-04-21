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

import mock

from apache.thermos.monitoring.detector import FixedPathDetector
from apache.thermos.observer.http.file_browser import TaskObserverFileBrowser
from apache.thermos.observer.task_observer import TaskObserver

MOCK_TASK_ID = 'abcd'
MOCK_BASE_PATH = '/a/b/c'
MOCK_FILENAME = 'd'


class PatchingTaskObserverFileBrowser(TaskObserverFileBrowser):
  def __init__(self, *args, **kw):
    self.__tasks = []
    super(PatchingTaskObserverFileBrowser, self).__init__()
    self._observer = self.make_observer()

  @classmethod
  def make_observer(cls):
    return TaskObserver(FixedPathDetector(MOCK_BASE_PATH))


def make_mocks(base_path=MOCK_BASE_PATH, filename=MOCK_FILENAME, process=None,
               logtype='stdout'):
  mock_logs = {
    'task_id': MOCK_TASK_ID,
    'filename': filename,
    'process': process,
    'run': 1,
    'logtype': logtype,
  }

  mock_file = {
    'task_id': MOCK_TASK_ID,
    'filename': filename,
  }
  mock_observer_logs = {
    'stdout': [os.path.join(base_path, filename), 'stdout'],
    'stderr': [os.path.join(base_path, filename), 'stderr'],
  }

  return mock_logs, mock_file, mock_observer_logs


class TestFileBrowser(object):

  def test_handle_logs_with_stdout(self):
    """ test observer handle_logs with logtype stdout """
    process = mock.Mock()
    mock_logs, _, mock_ologs = make_mocks(base_path=MOCK_BASE_PATH,
                                          filename=MOCK_FILENAME,
                                          process=process)
    with contextlib.nested(
      mock.patch('apache.thermos.observer.task_observer.TaskObserver.logs',
                 return_value=mock_ologs),
      mock.patch('apache.thermos.observer.task_observer.TaskObserver.valid_path',
                 return_value=(MOCK_BASE_PATH, MOCK_FILENAME))):
      ptfb = PatchingTaskObserverFileBrowser()
      actual_logs = ptfb.handle_logs(MOCK_TASK_ID, process, 1, 'stdout')

      assert mock_logs == actual_logs
      assert actual_logs['task_id'] == MOCK_TASK_ID
      assert actual_logs['filename'] == MOCK_FILENAME
      assert actual_logs['process'] == process
      assert actual_logs['run'] == 1
      assert actual_logs['logtype'] == 'stdout'

  def test_handle_logs_with_stderr(self):
    """ test observer handle_logs with logtype stderr """
    process = mock.Mock()
    mock_logs, _, mock_ologs = make_mocks(base_path=MOCK_BASE_PATH,
                                          filename=MOCK_FILENAME,
                                          process=process,
                                          logtype='stderr')
    with contextlib.nested(
      mock.patch('apache.thermos.observer.task_observer.TaskObserver.logs',
                 return_value=mock_ologs),
      mock.patch('apache.thermos.observer.task_observer.TaskObserver.valid_path',
                 return_value=(MOCK_BASE_PATH, MOCK_FILENAME))):
      ptfb = PatchingTaskObserverFileBrowser()
      actual_logs = ptfb.handle_logs(MOCK_TASK_ID, process, 1, 'stderr')

      assert mock_logs == actual_logs
      assert actual_logs['task_id'] == MOCK_TASK_ID
      assert actual_logs['filename'] == MOCK_FILENAME
      assert actual_logs['process'] == process
      assert actual_logs['run'] == 1
      assert actual_logs['logtype'] == 'stderr'

  def test_handle_file_with_stdout(self):
    """ test observer handle_file with logtype stdout """
    _, mock_file, mock_ologs = make_mocks(base_path=MOCK_BASE_PATH,
                                          filename=MOCK_FILENAME)
    ptfb = PatchingTaskObserverFileBrowser()
    actual_file = ptfb.handle_file(MOCK_TASK_ID, MOCK_FILENAME)

    assert mock_file == actual_file
    assert actual_file['task_id'] == MOCK_TASK_ID
    assert actual_file['filename'] == MOCK_FILENAME

  def test_handle_file_with_stderr(self):
    """ test observer handle_file with logtype stderr """
    _, mock_file, mock_ologs = make_mocks(base_path=MOCK_BASE_PATH,
                                          filename=MOCK_FILENAME,
                                          logtype='stderr')
    ptfb = PatchingTaskObserverFileBrowser()
    actual_file = ptfb.handle_file(MOCK_TASK_ID, MOCK_FILENAME)

    assert mock_file == actual_file
    assert actual_file['task_id'] == MOCK_TASK_ID
    assert actual_file['filename'] == MOCK_FILENAME
