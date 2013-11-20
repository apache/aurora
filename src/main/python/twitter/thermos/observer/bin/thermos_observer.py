from __future__ import print_function

import socket
import sys
import time

from twitter.common import app
from twitter.common.exceptions import ExceptionalThread
from twitter.common.http import HttpServer
from twitter.common.http.diagnostics import DiagnosticsEndpoints
from twitter.thermos.common.path import TaskPath
from twitter.thermos.observer.task_observer import TaskObserver
from twitter.thermos.observer.http.http_observer import BottleObserver


app.add_option("--root",
               dest="root",
               metavar="DIR",
               default=TaskPath.DEFAULT_CHECKPOINT_ROOT,
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

    task_observer = TaskObserver(opts.root)
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
