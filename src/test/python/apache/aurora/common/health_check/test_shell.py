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
import sys
import unittest

import mock

from apache.aurora.common.health_check.shell import ShellHealthCheck

# Recommended pattern for Python 2 and 3 support from https://github.com/google/python-subprocess32
# Backport which adds bug fixes and timeout support for Python 2.7
if os.name == 'posix' and sys.version_info[0] < 3:
  import subprocess32 as subprocess
else:
  # subprocess is included as part of Python standard lib in Python 3+.
  import subprocess


class TestHealthChecker(unittest.TestCase):

  @mock.patch('subprocess32.check_call')
  def test_health_check_ok(self, mock_sub):
    timeout = 30
    cmd = 'success cmd'
    shell = ShellHealthCheck(cmd, timeout_secs=timeout)
    success, msg = shell()
    self.assertTrue(success)
    self.assertIsNone(msg)
    mock_sub.assert_called_once_with(
      ['success', 'cmd'],
      timeout=30
    )

  @mock.patch('subprocess32.check_call')
  def test_health_check_failed(self, mock_sub):
    timeout = 30
    # Fail due to command returning a non-0 exit status.
    mock_sub.side_effect = subprocess.CalledProcessError(1, 'failed')
    cmd = 'cmd to fail'
    shell = ShellHealthCheck(cmd, timeout_secs=timeout)
    success, msg = shell()
    mock_sub.assert_called_once_with(
      ['cmd', 'to', 'fail'],
      timeout=30
    )
    self.assertFalse(success)
    self.assertEqual(msg, "Command 'failed' returned non-zero exit status 1")

  @mock.patch('subprocess32.check_call')
  def test_health_check_os_error(self, mock_sub):
    timeout = 30
    # Fail due to command not existing.
    mock_sub.side_effect = OSError(1, 'failed')
    cmd = 'cmd to not exist'
    shell = ShellHealthCheck(cmd, timeout_secs=timeout)
    success, msg = shell()
    mock_sub.assert_called_once_with(
      ['cmd', 'to', 'not', 'exist'],
      timeout=30
    )
    self.assertFalse(success)
    self.assertEqual(msg, 'OSError: failed')

  @mock.patch('subprocess32.check_call')
  def test_health_check_value_error(self, mock_sub):
    timeout = 30
    # Invalid commmand passed in raises ValueError.
    mock_sub.side_effect = ValueError('Could not read command.')
    cmd = 'defensive cmd'
    timeout = 10
    shell = ShellHealthCheck(cmd, timeout_secs=timeout)
    success, msg = shell()
    mock_sub.assert_called_once_with(
      ['defensive', 'cmd'],
      timeout=10
    )
    self.assertFalse(success)
    self.assertEqual(msg, 'Invalid commmand.')
