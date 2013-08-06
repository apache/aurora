from datetime import datetime
import json

from twitter.common import app
from twitter.mesos.client.api import MesosClientAPI
from twitter.mesos.client.base import check_and_log_response, handle_open
from twitter.mesos.client.deployment_api import AuroraDeploymentAPI
from twitter.mesos.common import AuroraJobKey
from twitter.mesos.packer import sd_packer_client

import argparse


class JobKeyAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    setattr(namespace, self.dest, AuroraJobKey.from_path(values))


class AuroraDeploymentCLI(object):
  """Parses subcommands to the deployment command and defer to the appropriate API calls
  """

  class CommandLineError(Exception): pass

  def __init__(self, clusters, deployment_api=None):
    self._deployment_api = deployment_api
    self._clusters = clusters
    self._scheduler_url = None

  def dispatch(self, args, options):
    parser = argparse.ArgumentParser(description="Manipulate deployments")
    verbs = parser.add_subparsers()

    self._add_create(verbs)
    self._add_log(verbs)
    self._add_release(verbs)
    self._add_reset(verbs)
    self._add_show(verbs)

    parsed_args = parser.parse_args(args)

    self._set_deployment_api(parsed_args, options.verbosity == 'verbose')

    # Call function bound with the parsed command (_create, _release, etc.)
    parsed_args.func(parsed_args)

  def _add_create(self, parser):
    create = parser.add_parser(
        'create', help='Create a job deployment')
    create.set_defaults(func=self._create)
    create.add_argument('job_key', action=JobKeyAction)
    create.add_argument('config_file', type=argparse.FileType('r'))
    create.add_argument('--message', '-m', help='An optional message to add to this deployment')
    self._add_release_flag(create)

  def _add_log(self, parser):
    log = parser.add_parser('log', help='Show the history of deployed configurations')
    log.set_defaults(func=self._log)
    log.add_argument('job_key', action=JobKeyAction)
    log.add_argument('--long', default=False, action='store_true', help="Show more details")

  def _add_release(self, parser):
    release = parser.add_parser(
        'release',
        help='Create or update a job to the latest created deployment')
    release.set_defaults(func=self._release)
    release.add_argument('job_key', action=JobKeyAction)
    self._add_release_argument_group(release)

  def _add_reset(self, parser):
    reset = parser.add_parser(
        'reset',
        help='Reset a deployment to an older version (use that to rollback to a particular version)')
    reset.set_defaults(func=self._reset)
    reset.add_argument('job_key', action=JobKeyAction)
    reset.add_argument('version_id', type=int)
    self._add_release_flag(reset)

  def _add_show(self, parser):
    show = parser.add_parser(
        'show',
        help='Show details of the job that would be released by running "deployment release"')
    show.set_defaults(func=self._show)
    show.add_argument('job_key', action=JobKeyAction)
    show.add_argument('version_id', nargs='?', default='latest')

  def _add_release_flag(self, parser):
    parser.add_argument(
        '-r', '--release', default=False, action='store_true',
        help='Create or update the job after uploading a new configuration file')
    parser.add_argument_group('release')
    self._add_release_argument_group(parser)

  def _add_release_argument_group(self, parser):
    kwargs = {
        'default': 3, 'type': int, 'help': 'Time interval between subsequent shard status checks.'}
    release_group = parser.add_argument_group('release')
    release_group.add_argument('--updater_health_check_interval_seconds', **kwargs)

  def _set_deployment_api(self, parsed_args, verbosity):
    cluster_name = parsed_args.job_key.cluster
    if cluster_name not in self._clusters:
      raise self.CommandLineError('No cluster named ' + cluster_name)

    if self._deployment_api is None:
      api = MesosClientAPI(self._clusters[cluster_name], verbose=verbosity)
      packer = sd_packer_client.create_packer(cluster_name, verbose=verbosity)
      self._deployment_api = AuroraDeploymentAPI(api, packer)
      self._scheduler_url = api.scheduler.scheduler().url

  def _create(self, args):
    job_key = args.job_key
    config_filename = args.config_file.name
    message = args.message

    self._deployment_api.create(job_key, config_filename, message)
    if args.release:
      self._release(args)

  def _log(self, args):
    job_key = args.job_key
    configs = self._deployment_api.log(job_key)
    printer = DeploymentConfigFormat.long_str if args.long else DeploymentConfigFormat.one_line_str
    print('\n'.join(printer(config) for config in configs))

  def _release(self, args):
    job_key = args.job_key
    updater_health_check_interval_seconds = args.updater_health_check_interval_seconds
    proxy_host = app.get_options().tunnel_host

    resp = self._deployment_api.release(job_key, updater_health_check_interval_seconds, proxy_host)
    check_and_log_response(resp)
    handle_open(
        self._scheduler_url,
        job_key.role,
        job_key.env,
        job_key.name)

  def _reset(self, args):
    job_key = args.job_key
    version_id = args.version_id
    proxy_host = app.get_options().tunnel_host

    self._deployment_api.reset(job_key, version_id, proxy_host)
    if args.release:
      self._release(args)

  def _show(self, args):
    job_key = args.job_key
    version_id = args.version_id
    proxy_host = app.get_options().tunnel_host

    (config, content) = self._deployment_api.show(job_key, version_id, proxy_host)
    print(DeploymentConfigFormat.full_str(config, content))

class DeploymentConfigFormat(object):
  _EMPTY_MESSAGE = "<Empty message>"

  @classmethod
  def full_str(cls, config, content):
    """Full representation of a deployment, with job content in JSON and raw pystachio template used
    to create the job"""

    parsed = json.loads(content)
    desc = []
    desc.append(cls.long_str(config))
    desc.append('Scheduled job:')
    desc.append('--')
    desc.append(json.dumps(json.loads(parsed.get('job')), indent=2))
    desc.append('--')
    files = parsed.get('loadables')
    for fname, content in files.items():
      desc.append('Raw file: %s' % fname)
      desc.append(content)
    return '\n'.join(desc)

  @classmethod
  def long_str(cls, config):
    """Multi line representation of a deployment, with audit trail"""

    released = ' (Currently released)' if config.released() else ''
    desc = []
    desc.append("Version: %s (md5: %s)%s" % (config.version_id, config.md5, released))
    desc.append("Created by: %s" % config.creation()['user'])
    desc.append("Date created: %s" % cls._timestamp_to_str(config.creation()['timestamp']))
    for release in config.releases():
      desc.append("Released by: %s" % release['user'])
      desc.append("Date released: %s" % cls._timestamp_to_str(release['timestamp']))
    desc.append("")
    desc.append(cls._indent_lines(cls._message(config), 4))
    return '\n'.join(desc) + '\n'

  @classmethod
  def one_line_str(cls, config):
    """One line representation of a deployment"""

    desc = []
    desc.append("%s" % config.version_id)
    desc.append("- %s" % cls._message(config).splitlines()[0])
    desc.append("(%s)" % cls._timestamp_to_str(config.creation()['timestamp']))
    desc.append("<%s>" % config.auditlog[0]['user'])
    if config.released():
      desc.append('(RELEASED)')
    return ' '.join(desc)

  @classmethod
  def _message(cls, config):
    return config.message if config.message else cls._EMPTY_MESSAGE

  @classmethod
  def _timestamp_to_str(cls, timestamp):
    return str(datetime.fromtimestamp(timestamp / 1000))

  @classmethod
  def _indent_lines(cls, s, n_spaces):
    return '\n'.join(n_spaces * ' ' + i for i in s.splitlines())
