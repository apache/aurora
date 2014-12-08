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

import logging
import sys

from apache.aurora.client.cli import CommandLine, ConfigurationPlugin
from apache.aurora.client.cli.logsetup import setup_default_log_handlers
from apache.aurora.client.cli.options import CommandOption


class AuroraLogConfigurationPlugin(ConfigurationPlugin):
  """Plugin for configuring log level settings for the aurora client."""

  def __init__(self):
    super(AuroraLogConfigurationPlugin, self).__init__()
    self.logging_level = None

  def get_options(self):
    return [
        CommandOption("--verbose-logging", "-v", default=False, action="store_true",
          help=("Show verbose logging, including all logs up to level INFO (equivalent to "
              "--logging-level=20)")),
        CommandOption("--logging-level", default=None, type=int, metavar="numeric_level",
          help="Set logging to a specific numeric level, using the standard python log-levels.")
    ]

  def before_dispatch(self, raw_args):
    # We need to process the loglevel arguments before dispatch, so that we can
    # do logging during dispatch. That means that we need to cheat, and manually
    # check for the logging arguments. (We still register them in get_options,
    # so that they'll show up in the help message.)
    loglevel = logging.WARN
    for arg in raw_args:
      if arg == "--verbose-logging" or arg == "-v":
        if loglevel > logging.INFO:
          loglevel = logging.INFO
      if arg.startswith("--logging-level="):
        arg_bits = arg.split("=")
        # If the format here is wrong, argparse will generate error, so we just skip
        # it if it's incorrect.
        if len(arg_bits) == 2:
          try:
            loglevel = int(arg_bits[1])
          except ValueError:
            print("Invalid value for log level; must be an integer, but got %s" % arg_bits[1],
                file=sys.stderr)
            raise
    setup_default_log_handlers(loglevel)
    self.logging_level = loglevel
    return raw_args

  def before_execution(self, context):
    context.logging_level = self.logging_level

  def after_execution(self, context, result_code):
    pass


class AuroraCommandLine(CommandLine):
  """The CommandLine implementation for the Aurora client v2 command line."""

  def __init__(self):
    super(AuroraCommandLine, self).__init__()
    self.register_plugin(AuroraLogConfigurationPlugin())

  @property
  def name(self):
    return 'aurora'

  @classmethod
  def get_description(cls):
    return 'Aurora client command line'

  def register_nouns(self):
    super(AuroraCommandLine, self).register_nouns()
    from apache.aurora.client.cli.cron import CronNoun
    self.register_noun(CronNoun())
    from apache.aurora.client.cli.jobs import Job
    self.register_noun(Job())
    from apache.aurora.client.cli.config import ConfigNoun
    self.register_noun(ConfigNoun())
    from apache.aurora.client.cli.quota import Quota
    self.register_noun(Quota())
    from apache.aurora.client.cli.sla import Sla
    self.register_noun(Sla())
    from apache.aurora.client.cli.task import Task
    self.register_noun(Task())
    from apache.aurora.client.cli.update import Update
    self.register_noun(Update())


def proxy_main():
  client = AuroraCommandLine()
  if len(sys.argv) == 1:
    sys.argv.append("help")
  sys.exit(client.execute(sys.argv[1:]))

if __name__ == '__main__':
  proxy_main()
