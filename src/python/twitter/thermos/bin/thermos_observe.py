import sys
import time

from twitter.common import app
from twitter.common.metrics import RootMetrics, LambdaGauge
from twitter.thermos.observer.observer import TaskObserver
from twitter.thermos.observer.http import BottleObserver

app.add_option("--root", dest = "root", metavar = "DIR",
               help = "root checkpoint directory for thermos task runners")
app.add_option("--port", dest = "port", type='int', metavar = "PORT", default=8051,
               help = "port to run observer webserver")

def register_varz():
  rm = RootMetrics()
  now = time.time()
  rm.register(LambdaGauge('uptime', lambda: time.time() - now))

def main(args):
  opts = app.get_options()
  register_varz()

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

  bo = BottleObserver(obs)
  bo.run('localhost', opts.port)

app.main()
