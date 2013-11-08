import socket
import sys
import time

from twitter.common import app
from twitter.common.app.modules.http import RootServer
from twitter.thermos.common.path import TaskPath
from twitter.thermos.observer.task_observer import TaskObserver
from twitter.thermos.observer.http.http_observer import BottleObserver


app.add_option("--root",
               dest="root",
               metavar="DIR",
               default=TaskPath.DEFAULT_CHECKPOINT_ROOT,
               help="root checkpoint directory for thermos task runners")


app.configure(module='twitter.common.app.modules.http',
    port=1338, host='0.0.0.0', enable=True, framework='cherrypy')


def main(args, opts):
  if args:
    print >> sys.stderr, "ERROR: unrecognized arguments: %s\n" % (" ".join(args))
    app.help()
    sys.exit(1)

  task_observer = TaskObserver(opts.root)
  task_observer.start()

  bottle_wrapper = BottleObserver(task_observer)
  RootServer().mount_routes(bottle_wrapper)

  # MainThread should just sleep forever.  TODO(wickman) Add something in
  # app that indicates sleeping forever until e.g.  a signal.
  while True:
    time.sleep(10)


app.main()
