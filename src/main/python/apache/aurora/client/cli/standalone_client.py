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

from apache.aurora.client.cli import CommandLine
from apache.aurora.client.cli.logsetup import setup_default_log_handlers


class AuroraCommandLine(CommandLine):
  """The CommandLine implementation for the Aurora client v2 command line."""

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


def proxy_main():
  client = AuroraCommandLine()
  setup_default_log_handlers(logging.INFO)
  if len(sys.argv) == 1:
    sys.argv.append("help")
  client.execute(sys.argv[1:])

if __name__ == '__main__':
  proxy_main()
