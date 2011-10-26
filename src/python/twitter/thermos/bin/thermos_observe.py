import sys
import time

from twitter.common import app
from twitter.common.app.modules.http import RootServer
from twitter.thermos.observer.observer import TaskObserver
from twitter.thermos.observer.http import BottleObserver

app.add_option("--root", dest = "root", metavar = "DIR",
               help = "root checkpoint directory for thermos task runners")

def main(args):
  opts = app.get_options()

  if args:
    print >> sys.stderr, "ERROR: unrecognized arguments: %s\n" % (" ".join(args))
    app.help()
    sys.exit(1)

  if not opts.root:
    print >> sys.stderr, "ERROR: must supply --root directory"
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
