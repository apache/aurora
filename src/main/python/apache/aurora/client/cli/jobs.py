from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.cli import (
    EXIT_INVALID_CONFIGURATION,
    Noun,
    Verb
)
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    BIND_OPTION,
    BROWSER_OPTION,
    CONFIG_OPTION,
    JOBSPEC_OPTION,
    JSON_OPTION
)
from apache.aurora.common.aurora_job_key import AuroraJobKey

from pystachio.config import Config


def parse_instances(instances):
  """Parse lists of instances or instance ranges into a set().

     Examples:
       0-2
       0,1-3,5
       1,3,5
  """
  if instances is None or instances == '':
    return None
  result = set()
  for part in instances.split(','):
    x = part.split('-')
    result.update(range(int(x[0]), int(x[-1]) + 1))
  return sorted(result)


class CreateJobCommand(Verb):
  @property
  def name(self):
    return 'create'

  @property
  def help(self):
    return 'Create a job using aurora'

  CREATE_STATES = ('PENDING', 'RUNNING', 'FINISHED')

  def setup_options_parser(self, parser):
    self.add_option(parser, BIND_OPTION)
    self.add_option(parser, BROWSER_OPTION)
    self.add_option(parser, JSON_OPTION)
    parser.add_argument('--wait_until', choices=self.CREATE_STATES,
        default='PENDING',
        help=('Block the client until all the tasks have transitioned into the requested state. '
                        'Default: PENDING'))
    self.add_option(parser, JOBSPEC_OPTION)
    self.add_option(parser, CONFIG_OPTION)

  def execute(self, context):
    try:
      config = context.get_job_config(context.options.jobspec, context.options.config_file)
    except Config.InvalidConfigError as e:
      raise context.CommandError(EXIT_INVALID_CONFIGURATION,
          'Error loading job configuration: %s' % e)
    api = context.get_api(config.cluster())
    monitor = JobMonitor(api, config.role(), config.environment(), config.name())
    resp = api.create_job(config)
    context.check_and_log_response(resp)
    if context.options.open_browser:
      context.open_job_page(api, config)
    if context.options.wait_until == 'RUNNING':
      monitor.wait_until(monitor.running_or_finished)
    elif context.options.wait_until == 'FINISHED':
      monitor.wait_until(monitor.terminal)


class KillJobCommand(Verb):
  @property
  def name(self):
    return 'kill'

  def setup_options_parser(self, parser):
    self.add_option(parser, BROWSER_OPTION)
    parser.add_argument('--instances', type=parse_instances, dest='instances', default=None,
        help='A list of instance ids to act on. Can either be a comma-separated list (e.g. 0,1,2) '
            'or a range (e.g. 0-2) or any combination of the two (e.g. 0-2,5,7-9). If not set, '
            'all instances will be acted on.')
    parser.add_argument('--config', type=str, default=None, dest='config',
         help='Config file for the job, possibly containing hooks')
    self.add_option(parser, JOBSPEC_OPTION)

  def execute(self, context):
    api = context.get_api(context.options.jobspec.cluster)
    resp = api.kill_job(context.options.jobspec, context.options.instances)
    context.check_and_log_response(resp)
    context.handle_open(api)


class Job(Noun):
  @property
  def name(self):
    return 'job'

  @property
  def help(self):
    return "Work with an aurora job"

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(Job, self).__init__()
    self.register_verb(CreateJobCommand())
    self.register_verb(KillJobCommand())
