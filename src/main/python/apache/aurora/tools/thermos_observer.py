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

"""A Mesos-customized entry point to the thermos_observer webserver."""

import sys
import thread
import threading
import time

from twitter.common import app, log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time

from apache.aurora.executor.common.path_detector import MesosPathDetector
from apache.thermos.observer.http.configure import configure_server
from apache.thermos.observer.task_observer import TaskObserver

app.add_option(
    '--mesos-root',
    dest='mesos_root',
    type='string',
    default=MesosPathDetector.DEFAULT_MESOS_ROOT,
    help='The mesos root directory to search for Thermos executor sandboxes [default: %default]')


app.add_option(
    '--ip',
    dest='ip',
    type='string',
    default='0.0.0.0',
    help='The IP address the observer will bind to.')


app.add_option(
    '--port',
    dest='port',
    type='int',
    default=1338,
    help='The port on which the observer should listen.')


app.add_option(
    '--polling_interval_secs',
      dest='polling_interval_secs',
      type='int',
      default=int(TaskObserver.POLLING_INTERVAL.as_(Time.SECONDS)),
      help='The number of seconds between observer refresh attempts.')


# Allow an interruptible sleep so that ^C works.
def sleep_forever():
  while True:
    time.sleep(1)


def initialize(options):
  path_detector = MesosPathDetector(options.mesos_root)
  polling_interval = Amount(options.polling_interval_secs, Time.SECONDS)
  return TaskObserver(path_detector, interval=polling_interval)


def handle_error(exc_type, value, traceback):
  """ Tear down the observer in case of unhandled errors.

  By using ExceptionalThread throughout the observer we have ensured that sys.excepthook will
  be called for every unhandled exception, even for those not originating in the main thread.
  """
  log.error("An unhandled error occured. Tearing down.", exc_info=(exc_type, value, traceback))
  # TODO: In Python 3.4 we will be able to use threading.main_thread()
  if not isinstance(threading.current_thread(), threading._MainThread):
    thread.interrupt_main()


def main(_, options):
  observer = initialize(options)
  observer.start()
  root_server = configure_server(observer)

  server = ExceptionalThread(target=lambda: root_server.run(options.ip, options.port, 'cherrypy'))
  server.daemon = True
  server.start()

  sleep_forever()


sys.excepthook = handle_error
LogOptions.set_stderr_log_level('google:INFO')
app.main()
