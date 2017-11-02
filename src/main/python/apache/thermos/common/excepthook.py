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
import thread
import threading

from twitter.common import app, log


class ExceptionTerminationHandler(app.Module):
  """ An application module tha will terminate the app process in case of unhandled errors.

  By using ExceptionalThread throughout the codebase we have ensured that sys.excepthook will
  be called for every unhandled exception, even for those not originating in the main thread.
  """

  def __init__(self):
    app.Module.__init__(self, __name__, description="Exception termination handler.")

  def setup_function(self):
    self._saved_hook = sys.excepthook

    def teardown_handler(exc_type, value, trace):
      if isinstance(threading.current_thread(), threading._MainThread):
        self._former_hook()(exc_type, value, trace)
      else:
        try:
          log.error("Unhandled error in %s. Interrupting main thread.", threading.current_thread())
          self._former_hook()(exc_type, value, trace)
        finally:
          thread.interrupt_main()

    sys.excepthook = teardown_handler

  def _former_hook(self):
    return getattr(self, '_saved_hook', sys.__excepthook__)

  def teardown_function(self):
    sys.excepthook = self._former_hook()
