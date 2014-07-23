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
import sys
import traceback
from threading import Thread

from twitter.common.decorators import identify_thread

try:
  from Queue import Queue
except ImportError:
  from queue import Queue


class ExecutionError(Exception):
  """Unhandled thread error wrapper. Raised on the calling thread."""
  pass


class ErrorHandlingThread(Thread):
  """A thread that helps with unhandled exceptions by re-raising errors
  with the parent thread upon completion."""

  def __init__(self, *args, **kw):
    super(ErrorHandlingThread, self).__init__(*args, **kw)
    self.__real_run, self.run = self.run, self._excepting_run
    self.__errors = Queue()

  @identify_thread
  def _excepting_run(self, *args, **kw):
    try:
      self.__real_run(*args, **kw)
      self.__errors.put(None)
    except Exception:
      try:
        e_type, e_val, e_tb = sys.exc_info()
        self.__errors.put(ExecutionError(
            'Unhandled error while running worker thread. '
            'Original error details: %s' % traceback.format_exception(e_type, e_val, e_tb)))
      except: # noqa
        # This appears to be the only way to avoid nasty "interpreter shutdown" errors when
        # dealing with daemon threads. While not ideal, there is nothing else we could do here
        # if the sys.exc_info() call fails.
        pass

  def join_and_raise(self):
    """Waits for completion and re-raises any exception on a caller thread."""
    error = self.__errors.get(timeout=sys.maxint)  # Timeout for interruptibility.
    if error is not None:
      raise error


def spawn_worker(target, *args, **kwargs):
  """Creates and starts a new daemon worker thread.

  Arguments:
  target -- target method.

  Returns thread handle.
  """
  thread = ErrorHandlingThread(target=target, *args, **kwargs)
  thread.daemon = True
  thread.start()
  return thread
