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

import sys

from twitter.common import app
from twitter.common.exceptions import ExceptionalThread
from twitter.common.http import HttpServer
from twitter.common.http.diagnostics import DiagnosticsEndpoints

from apache.thermos.common.constants import DEFAULT_CHECKPOINT_ROOT
from apache.thermos.monitoring.detector import FixedPathDetector
from apache.thermos.observer.http.http_observer import BottleObserver
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


def proxy_main():
  def main(args, opts):
    if args:
      print("ERROR: unrecognized arguments: %s\n" % (" ".join(args)), file=sys.stderr)
      app.help()
      sys.exit(1)

    root_server = HttpServer()
    root_server.mount_routes(DiagnosticsEndpoints())

    task_observer = TaskObserver(FixedPathDetector(opts.root))
    task_observer.start()

    bottle_wrapper = BottleObserver(task_observer)

    root_server.mount_routes(bottle_wrapper)

    def run():
      root_server.run('0.0.0.0', opts.port, 'cherrypy')

    et = ExceptionalThread(target=run)
    et.daemon = True
    et.start()
    et.join()

  app.main()
