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

from __future__ import print_function

import time

from twitter.common import app
from twitter.common.exceptions import ExceptionalThread

from apache.thermos.common.constants import DEFAULT_CHECKPOINT_ROOT
from apache.thermos.monitoring.detector import FixedPathDetector
from apache.thermos.observer.http.configure import configure_server
from apache.thermos.observer.task_observer import TaskObserver

app.add_option("--root",
               dest="root",
               metavar="DIR",
               default=DEFAULT_CHECKPOINT_ROOT,
               help="root checkpoint directory for thermos task runners")


app.add_option("--port",
               dest="port",
               metavar="INT",
               default=1338,
               help="port number to listen on.")


def sleep_forever():
  while True:
    time.sleep(1)


def run():
  def main(_, opts):
    path_detector = FixedPathDetector(opts.root)
    task_observer = TaskObserver(path_detector)
    task_observer.start()
    server = configure_server(task_observer)

    thread = ExceptionalThread(target=lambda: server.run('0.0.0.0', opts.port, 'cherrypy'))
    thread.daemon = True
    thread.start()

    sleep_forever()

  app.main()
