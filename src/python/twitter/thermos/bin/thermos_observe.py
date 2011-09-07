import sys

from twitter.common import app
from twitter.thermos.observer.observer import TaskObserver
from twitter.thermos.observer.http     import ObserverHttpHandler

app.add_option("--root", dest = "root", metavar = "DIR",
               help = "root checkpoint directory for thermos task runners")
app.add_option("--port", dest = "port", type='int', metavar = "PORT", default=8051,
               help = "port to run observer webserver")

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

  obs = TaskObserver(opts.root)
  obs.start()

  # this runs forever
  ObserverHttpHandler('localhost', opts.port, obs)

app.main()
