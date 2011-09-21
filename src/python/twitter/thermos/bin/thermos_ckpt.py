import os
import sys
import pprint

from gen.twitter.thermos.ttypes import TaskRunnerState, TaskRunnerCkpt

from twitter.common import app
from twitter.common.recordio import ThriftRecordReader
from twitter.thermos.base.ckpt import TaskCkptDispatcher

app.add_option("--checkpoint", dest = "ckpt", metavar = "CKPT",
               help = "read checkpoint from CKPT")
app.add_option("--assemble", dest = "assemble", metavar = "CKPT", default=True,
               help = "read checkpoint from CKPT")

def main(args):
  values = app.get_options()

  if len(args) > 0:
    print >> sys.stderr, "ERROR: unrecognized arguments: %s\n" % (" ".join(args))
    app.help()
    sys.exit(1)

  if not values.ckpt:
    print >> sys.stderr, "ERROR: must supply --checkpoint"
    app.help()
    sys.exit(1)

  fp = file(values.ckpt, "r")
  rr = ThriftRecordReader(fp, TaskRunnerCkpt)
  wrs = TaskRunnerState(processes = {})
  dispatcher = TaskCkptDispatcher()
  for wts in rr:
    print 'Recovering: ', wts
    if values.assemble is True:
       dispatcher.update_runner_state(wrs, wts)
  print '\n\n\n'
  if values.assemble:
    print 'Recovered Task'
    pprint.pprint(wrs.header)

    print '\nRecovered Task State'
    pprint.pprint(wrs.state)

    print '\nRecovered Allocated Ports'
    pprint.pprint(wrs.ports)

    print '\nRecovered Processes'
    pprint.pprint(wrs.processes)

app.main()
