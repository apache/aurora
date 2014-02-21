import sys

from apache.aurora.client.cli import CommandLine
from apache.aurora.client.cli.bridge import Bridge, CommandProcessor


class AuroraCommandLine(CommandLine):
  """The CommandLine implementation for the Aurora client v2 command line."""

  @classmethod
  def get_description(cls):
    return 'Aurora client command line'

  def register_nouns(self):
    super(AuroraCommandLine, self).register_nouns()
    from apache.aurora.client.cli.jobs import Job
    self.register_noun(Job())
    from apache.aurora.client.cli.quota import Quota
    self.register_noun(Quota())
    from apache.aurora.client.cli.sla import Sla
    self.register_noun(Sla())
    from apache.aurora.client.cli.task import Task
    self.register_noun(Task())


class AuroraClientV2CommandProcessor(CommandProcessor):
  def __init__(self):
    self.commandline = AuroraCommandLine()

  @property
  def name(self):
    return "Aurora Client v2"

  def get_commands(self):
    return self.commandline.registered_nouns

  def execute(self, args):
    return self.commandline.execute(args[1:])


class AuroraClientV1CommandProcessor(CommandProcessor):
  # TODO(mchucarroll): deprecate client v1. (AURORA-131)

  @property
  def name(self):
    return "Aurora Client v1"

  def get_commands(self):
    return ["cancel_update", "create", "diff", "get_quota", "inspect", "kill", "list_jobs",
        "open", "restart", "run", "ssh", "start_cron", "status", "update", "version" ]

  def execute(self, args):
    from apache.aurora.client.bin.aurora_client import proxy_main as clientone_proxy_main
    return clientone_proxy_main()


def proxy_main():
  v2 = AuroraClientV2CommandProcessor()
  v1 = AuroraClientV1CommandProcessor()
  bridge = Bridge([v2, v1], default=v1)
  bridge.execute(sys.argv)

if __name__ == '__main__':
  proxy_main()
