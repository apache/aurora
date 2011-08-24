import os
import sys
import pprint

from thermos_thrift.ttypes import TaskRunnerState, TaskRunnerCkpt

from twitter.common import options
from twitter.common.recordio import ThriftRecordReader
from twitter.thermos.base.ckpt import TaskCkptDispatcher

def parse_commandline():
  options.add("--checkpoint", dest = "ckpt", metavar = "CKPT",
              help = "read checkpoint from CKPT")
  options.add("--assemble", dest = "assemble", metavar = "CKPT", default=True,
              help = "read checkpoint from CKPT")

  (values, args) = options.parse()

  if len(args) > 0:
    print >> sys.stderr, "ERROR: unrecognized arguments: %s\n" % (" ".join(args))
    options.print_help(sys.stderr)
    sys.exit(1)

  if not values.ckpt:
    print >> sys.stderr, "ERROR: must supply --checkpoint"
    options.print_help(sys.stderr)
    sys.exit(1)

  return (values, args)

def main():
  values, _ = parse_commandline()

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

if __name__ == '__main__':
  main()
