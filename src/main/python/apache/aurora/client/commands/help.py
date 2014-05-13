#
# Copyright 2013 Apache Software Foundation
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

from __future__ import print_function

import collections
import sys

from twitter.common import app

from apache.aurora.client.base import die


def make_commands_str(commands):
  commands.sort()
  if len(commands) == 1:
    return str(commands[0])
  elif len(commands) == 2:
    return '%s (or %s)' % (str(commands[0]), str(commands[1]))
  else:
    return '%s (or any of: %s)' % (str(commands[0]), ' '.join(map(str, commands[1:])))


def generate_full_usage():
  docs_to_commands = collections.defaultdict(list)
  for (command, doc) in app.get_commands_and_docstrings():
    if doc is not None:
      docs_to_commands[doc].append(command)
  def make_docstring(item):
    (doc_text, commands) = item
    def format_line(line):
      return '    %s\n' % line.lstrip()
    stripped = ''.join(map(format_line, doc_text.splitlines()))
    return '%s\n%s' % (make_commands_str(commands), stripped)
  usage = sorted(map(make_docstring, docs_to_commands.items()))
  return 'Available commands:\n\n' + '\n'.join(usage)


@app.command
def help(args):
  """usage: help [subcommand]

  Prints help for using the aurora client, or one of its specific subcommands.
  """
  if not args:
    print(generate_full_usage())
    sys.exit(0)

  if len(args) > 1:
    die('Please specify at most one subcommand.')

  subcmd = args[0]
  if subcmd in app.get_commands():
    app.command_parser(subcmd).print_help()
  else:
    print('Subcommand %s not found.' % subcmd)
    sys.exit(1)
