import json
from tempfile import NamedTemporaryFile

from twitter.common import log
from twitter.mesos.client.config import get_config
from twitter.mesos.config import AuroraConfig, AuroraConfigLoader
from twitter.mesos.packer.packer_client import Packer

from gen.twitter.mesos.constants import ACTIVE_STATES


class AuroraStageAPI(object):
  """API for manipulating staged configurations for the aurora scheduler.
  """

  class NotStagedError(Exception): pass

  def __init__(self, api, packer):
    self._api = api
    self._packer = packer

  def _config_package_name(self, job_key):
    return '__job_%s_%s_%s' % (job_key.cluster, job_key.env, job_key.name)

  def create(self, job_key, config_filename, message=None):
    log.info('Staging job configuration: %s' % job_key)
    pkg_name = self._config_package_name(job_key)

    config = get_config(
        job_key.name,
        config_filename,
        False,
        False,
        None,
        select_cluster=job_key.cluster,
        select_env=job_key.env)

    file_content = json.dumps({
        'loadables': AuroraConfigLoader(config_filename).loadables,
        'job': config.raw().json_dumps(),
    })
    metadata = json.dumps({} if message is None else {'message': message})

    with NamedTemporaryFile(prefix='job_description_') as job_file:
      log.debug('Writing configuration loadables to temporary file: %s' % job_file.name)
      job_file.write(file_content)
      job_file.flush()
      self._packer.add(job_key.role, pkg_name, job_file.name, metadata)

  def log(self, job_key):
    config_pkg_name = self._config_package_name(job_key)
    try:
      versions = self._packer.list_versions(job_key.role, config_pkg_name)
    except Packer.Error as e:
      self._handle_packer_error(e, job_key)

    return StagedConfig.from_packer_list(versions)

  def _is_running(self, job_key):
    resp = self._api.check_status(job_key)
    return resp.tasks and any(task.status in ACTIVE_STATES for task in resp.tasks)

  def release(self, job_key, health_check_interval_seconds, proxy_host):
    config_pkg_name = self._config_package_name(job_key)
    with NamedTemporaryFile(prefix='job_configuration_') as job_file:
      try:
        pkg = self._packer.get_version(job_key.role, config_pkg_name, 'latest')
        self._packer.fetch(
            job_key.role, config_pkg_name, str(pkg['id']), proxy_host, job_file)
      except Packer.Error as e:
        self._handle_packer_error(e, job_key)

      job_file.seek(0)
      staged_json = json.load(job_file)

      config = AuroraConfig.loads_json(
          staged_json['job'],
          select_cluster=job_key.cluster,
          select_env=job_key.env)

    if self._is_running(job_key):
      log.info('Active tasks found for job %s. Updating existing job' % job_key)
      resp = self._api.update_job(
          config, health_check_interval_seconds=health_check_interval_seconds)
    else:
      log.info('No active tasks found for job %s. Creating new job' % job_key)
      resp = self._api.create_job(config)
    self._packer.set_live(job_key.role, config_pkg_name, str(pkg['id']))
    return resp

  def _handle_packer_error(self, e, job_key):
    if 'Requested package or version not found' in str(e):
      raise self.NotStagedError(
          "Cannot find a staged configuration for job %s. Run 'mesos stage' first." % job_key)
    else:
      raise


class StagedConfig(object):
  """Helper structure to work with staged configurations"""

  @classmethod
  def from_packer_list(cls, packer_versions):
    """Returns a list of StagedConfig from a packer json structure"""
    return [cls(
        version['id'],
        version['md5sum'],
        version['auditLog'],
        version['metadata'] or '')  # Metadata can be None
        for version in packer_versions]

  def __init__(self, version_id, md5, auditlog, metadata):
    self.version_id = version_id
    self.md5 = md5
    self.auditlog = auditlog
    self.message = ''
    try:
      loaded_metadata = json.loads(metadata)
      if isinstance(loaded_metadata, dict):
        self.message = loaded_metadata.get('message', '')
      else:
        raise ValueError
    except ValueError:
      log.warning('Unexpected value in staged config metadata: %s' % metadata)

  def creation(self):
    """Returns the creation event"""
    return self.auditlog[0]

  def releases(self):
    """List of release events for this staged configuration"""
    return [log for log in self.auditlog if log['state'] == 'LIVE']

  def released(self):
    """Return True iff this staged configuration is currently released"""
    return self.auditlog[-1]['state'] == 'LIVE'
