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
import logging
import sys

from apache.aurora.client.cli.bridge import Bridge, CommandProcessor
from apache.aurora.client.cli.logsetup import setup_default_log_handlers
from apache.aurora.client.cli.standalone_client import AuroraCommandLine


# TODO(mchucarroll): the entire bridged executable mechanism here is
# intended to be deprecated once clientv2 has proven to be stable
# and adopted by most users. Once we reach that point, this should
# be replaced by the standalone executable defined in standalone_client.py.
class AuroraClientV2CommandProcessor(CommandProcessor):
  def __init__(self):
    self.commandline = AuroraCommandLine()

  @property
  def name(self):
    return "Aurora Client v2"

  def get_commands(self):
    return self.commandline.registered_nouns

  def execute(self, args):
    setup_default_log_handlers(logging.INFO)
    return self.commandline.execute(args[1:])


class AuroraClientV1CommandProcessor(CommandProcessor):
  # TODO(mchucarroll): deprecate client v1. (AURORA-131)

  @property
  def name(self):
    return "Aurora Client v1"

  def show_help(self):
    print("Supported commands are: %s" % ", ".join(self.get_commands()))
    print("\nRun '%s help _command_' for help on a specific command" % sys.argv[0])

  def get_commands(self):
    return ["cancel_update", "create", "diff", "get_quota", "inspect", "kill", "list_jobs",
        "open", "restart", "run", "ssh", "start_cron", "status", "update", "version"]

  def execute(self, args):
    from apache.aurora.client.bin.aurora_client import proxy_main as clientone_proxy_main
    return clientone_proxy_main()


def proxy_main():
  v2 = AuroraClientV2CommandProcessor()
  v1 = AuroraClientV1CommandProcessor()
  bridge = Bridge([v2, v1], default=v1)
  sys.exit(bridge.execute(sys.argv))


if __name__ == '__main__':
  proxy_main()
