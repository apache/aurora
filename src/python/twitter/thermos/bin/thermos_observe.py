import sys
import getpass

import logging
import twitter.common.log
from twitter.common import options

from twitter.thermos.observer.observer import TaskObserver
from twitter.thermos.observer.http     import ObserverHttpHandler

def parse_commandline():
  options.add("--root", dest = "root", metavar = "DIR",
              help = "root checkpoint directory for thermos task runners")
  options.add("--port", dest = "port", type='int', metavar = "PORT", default=8051,
              help = "port to run observer webserver")

  (opts, args) = options.parse()

  if args:
    print >> sys.stderr, "ERROR: unrecognized arguments: %s\n" % (" ".join(args))
    options.print_help(sys.stderr)
    sys.exit(1)

  if not opts.root:
    print >> sys.stderr, "ERROR: must supply --root directory"
    options.print_help(sys.stderr)
    sys.exit(1)

  return (opts, args)

def main():
  opts, _ = parse_commandline()

  twitter.common.log.init("thermos_observer")

  obs = TaskObserver(opts.root)
  obs.start()

  # this runs forever
  ObserverHttpHandler('localhost', opts.port, obs)

if __name__ == '__main__':
  main()
