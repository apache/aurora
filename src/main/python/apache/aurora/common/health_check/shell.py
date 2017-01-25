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

from subprocess32 import STDOUT

# Recommended pattern for Python 2 and 3 support from https://github.com/google/python-subprocess32
# Backport which adds bug fixes and timeout support for Python 2.7
if os.name == 'posix' and sys.version_info[0] < 3:
  import subprocess32 as subprocess
else:
  # subprocess is included as part of Python standard lib in Python 3+.
  import subprocess


class WrappedCalledProcessError(subprocess.CalledProcessError):
  """
  Wraps a CalledProcessError but overrides the command so that in the event it was run through an
  isolator, the original command is exposed to the user, rather than the isolated value.
  """

  def __init__(self, original_command, error):
    self.cmd = original_command
    self.returncode = error.returncode
    self.output = error.output

  def __str__(self):
    return "Command '%s' returned non-zero exit status %d with output '%s'" % (
      self.cmd, self.returncode, self.output)


class ShellHealthCheck(object):

  def __init__(
        self,
        raw_cmd,
        wrapped_cmd,
        preexec_fn=None,
        timeout_secs=None):

    """
    Initialize with the command we would like to call.
    :param raw_cmd: Command to execute as passed by the user. Supposed to 0 return code on success.
    :type raw_cmd: str
    :param wrapped_cmd: Wrapped form of the user command including executing shell.
    :type wrapped_cmd: list(str)
    :param preexec_fn: Callable to invoke just before the child shell process is executed.
    :type preexec_fn: callable
    :param timeout_secs: Timeout in seconds.
    :type timeout_secs: int
    """
    self._raw_cmd = raw_cmd
    self._wrapped_cmd = wrapped_cmd
    self._preexec_fn = preexec_fn
    self._timeout_secs = timeout_secs

  def __call__(self):
    """
    Call a shell command line health check.

    :return: A tuple of (bool, str)
    :rtype tuple:
    """
    try:
      subprocess.check_output(
          self._wrapped_cmd,
          timeout=self._timeout_secs,
          preexec_fn=self._preexec_fn,
          stderr=STDOUT)
      return True, None
    except subprocess.CalledProcessError as reason:
      # The command didn't return a 0 so provide reason for failure.
      return False, str(WrappedCalledProcessError(self._raw_cmd, reason))
    except subprocess.TimeoutExpired:
      return False, 'Health check timed out.'
    except OSError as e:
      return False, 'OSError: %s' % e.strerror
    except ValueError:
      return False, 'Invalid commmand.'
