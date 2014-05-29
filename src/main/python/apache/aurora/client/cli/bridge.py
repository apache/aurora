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
import sys


class CommandProcessor(object):
  """A wrapper for anything which can receive a set of command-line parameters and execute
  something using them.

  This is built assuming that the first command-line parameter is the name of
  a command to be executed. For example, if this was being used to build a command-line
  tool named "tool", then a typical invocation from the command-line would look like
  "tool cmd arg1 arg2". "cmd" would be the name of the command to execute, and
  "arg1" and "arg2" would be the parameters to that command.
  """

  @property
  def name(self):
    """Get the name of this command processor"""

  def execute(self, args):
    """Execute the command-line tool wrapped by this processor.

    :param args: a list of the parameters used to invoke the command. Typically,
        this will be sys.argv.
    """
    pass

  def get_commands(self):
    """Get a list of the commands that this processor can handle."""
    pass


class Bridge(object):
  """Given multiple command line programs, each represented by a "CommandProcessor" object,
  refer command invocations to the command line that knows how to process them.
  """

  def __init__(self, command_processors, default=None):
    """
    :param command_processors: a list of command-processors.
    :param default: the default command processor. any command which is not
      reported by "get_commands" as part of any of the registered processors
      will be passed to the default.
    """
    self.command_processors = command_processors
    self.default = default

  def show_help(self, args):
    """Dispatch a help request to the appropriate sub-command"""
    if len(args) == 2:  # command was just "help":
      print("This is a merged command line, consisting of %s" %
          [cp.name for cp in self.command_processors])
      for cp in self.command_processors:
        print("========== help for %s ==========" % cp.name)
        cp.execute(args)
      return 0
    elif len(args) >= 3:
      discriminator = args[2]
      for cp in self.command_processors:
        if discriminator in cp.get_commands():
          return cp.execute(args)
      if self.default is not None:
        return self.default.execute(args)


  def execute(self, args):
    """Dispatch a command line to the appropriate CommandProcessor"""
    if len(args) == 1:
      args.append('help')
    for cl in self.command_processors:
      if args[1] == 'help' or args[1] == '--help':
        self.show_help(args)
        return 0
      if args[1] in cl.get_commands():
        return cl.execute(args)
    if self.default is not None:
      return self.default.execute(args)
    else:
      print('Unknown command: %s' % args[1])
      sys.exit(1)
