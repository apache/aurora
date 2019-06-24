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

from twitter.common.log.formatters.plain import PlainFormatter

from apache.aurora.client.binding_helper import BindingHelper
from apache.aurora.client.binding_helpers.docker_helper import DockerBindingHelper
from apache.aurora.client.cli import CommandLine, ConfigurationPlugin
from apache.aurora.client.cli.options import CommandOption
from apache.aurora.common.auth.auth_module_manager import register_auth_module


class AuroraLogConfigurationPlugin(ConfigurationPlugin):
  """Plugin for configuring log level settings for the aurora client."""

  def get_options(self):
    return [
      CommandOption("--verbose",
                    "-v",
                    default=False,
                    action="store_true",
                    help=("Show verbose output"))
    ]

  def before_dispatch(self, raw_args):
    # TODO(zmanji): Consider raising the default log level to WARN.
    loglevel = logging.INFO
    for arg in raw_args:
      if arg == "--verbose" or arg == "-v":
        loglevel = logging.DEBUG

    logging.getLogger().setLevel(loglevel)
    handler = logging.StreamHandler()
    handler.setFormatter(PlainFormatter())
    logging.getLogger().addHandler(handler)
    self._configure_lib_logging(loglevel)
    return raw_args

  def before_execution(self, context):
    pass

  def after_execution(self, context, result_code):
    pass

  def _configure_lib_logging(self, loglevel):
    """Sets logging level for "chatty" third party libs.

    Some dependencies have low default logging threshold thus generating messages that could
    be confusing to users under normal conditions. To mitigate, we set the default loglevel
    to CRITICAL to filter out the noise and re-enable logging when verbose output is requested.
    """
    lib_loglevel = logging.DEBUG if loglevel == logging.DEBUG else logging.CRITICAL
    logging.getLogger("requests_kerberos").setLevel(lib_loglevel)


class AuroraAuthConfigurationPlugin(ConfigurationPlugin):
  """Plugin for configuring aurora client authentication."""

  def get_options(self):
    return []

  def before_dispatch(self, raw_args):
    return raw_args

  def before_execution(self, context):
    try:
      from apache.aurora.kerberos.auth_module import KerberosAuthModule
      register_auth_module(KerberosAuthModule())
    except ImportError:
      # Use default auth implementation if kerberos is not available.
      pass

  def after_execution(self, context, result_code):
    pass


class AuroraHelpersPlugin(ConfigurationPlugin):
  """Plugin for configuring binding helpers."""

  def get_options(self):
    return []

  def before_dispatch(self, raw_args):
    return raw_args

  def before_execution(self, context):
    BindingHelper.register(DockerBindingHelper())

  def after_execution(self, context, result_code):
    pass


class AuroraCommandLine(CommandLine):
  """The CommandLine implementation for the Aurora client command line."""

  def __init__(self):
    super(AuroraCommandLine, self).__init__()
    self.register_plugin(AuroraLogConfigurationPlugin())
    self.register_plugin(AuroraAuthConfigurationPlugin())
    self.register_plugin(AuroraHelpersPlugin())

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
  # Defaulting to '-h' results in a similar, but more inviting message than 'too few arguments'.
  if len(sys.argv) == 1:
    sys.argv.append('-h')
  sys.exit(client.execute(sys.argv[1:]))


if __name__ == '__main__':
  proxy_main()
