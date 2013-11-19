import copy
import sys
import json
import pprint

from twitter.common import app
from twitter.thermos.config.loader import ThermosConfigLoader

def main(args):
  """
    Given .thermos configs, loads them and prints out information about them.
  """

  if len(args) == 0:
    app.help()

  for arg in args:
    print '\nparsing %s\n' % arg
    tc = ThermosConfigLoader.load(arg)

    for task_wrapper in tc.tasks():
      task = task_wrapper.task
      if not task.has_name():
        print 'Found unnamed task!  Skipping...'
        continue

      print 'Task: %s [check: %s]' % (task.name(), task.check())
      if not task.processes():
        print '  No processes.'
      else:
        print '  Processes:'
        for proc in task.processes():
          print '    %s' % proc

      ports = task_wrapper.ports()
      if not ports:
        print '  No unbound ports.'
      else:
        print '  Ports:'
        for port in ports:
          print '    %s' % port

      print 'raw:'
      pprint.pprint(json.loads(task_wrapper.to_json()))

app.set_usage("%s config1 config2 ..." % app.name())
app.main()
