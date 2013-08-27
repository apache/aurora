import json
from tempfile import NamedTemporaryFile

from twitter.common import log

from twitter.aurora.client.config import get_config
from twitter.aurora.config import AuroraConfig, AuroraConfigLoader
from twitter.packer.packer_client import Packer

from gen.twitter.aurora.constants import ACTIVE_STATES


class AuroraDeploymentAPI(object):
  """API for manipulating deployments for the aurora scheduler.
  """

  class Error(Exception): pass

  class NoDeploymentError(Error):
    def __init__(self, job_key):
      super(AuroraDeploymentAPI.NoDeploymentError, self).__init__(
          'Cannot find a deployment configuration for job %s. '
          'Run "mesos deployment create" first.' % job_key)

  class NoSuchVersion(Error):
    def __init__(self, job_key, version):
      super(AuroraDeploymentAPI.NoSuchVersion, self).__init__(
          "No deployment version %s for job %s" % (version, job_key))

  def __init__(self, api, packer):
    self._api = api
    self._packer = packer

  def _config_package_name(self, job_key):
    return '__job_%s_%s_%s' % (job_key.cluster, job_key.env, job_key.name)

  def create(self, job_key, config_filename, message=None):
    log.info('Creating deployment configuration: %s' % job_key)
    pkg_name = self._config_package_name(job_key)

    config = get_config(
        job_key.name,
        config_filename,
        False,
        False,
        None,
        select_cluster=job_key.cluster,
        select_role=job_key.role,
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

    latest = self._packer.get_version(job_key.role, self._config_package_name(job_key), 'latest')
    return DeploymentConfig.from_packer_list(versions, latest['id'])

  def _is_running(self, job_key):
    resp = self._api.check_status(job_key)
    return resp.tasks and any(task.status in ACTIVE_STATES for task in resp.tasks)

  def release(self, job_key, health_check_interval_seconds, proxy_host):
    config, content = self._fetch_full_config(job_key, 'latest', proxy_host)

    job_config = AuroraConfig.loads_json(
        content['job'],
        select_cluster=job_key.cluster,
        select_env=job_key.env)

    if self._is_running(job_key):
      log.info('Active tasks found for job %s. Updating existing job' % job_key)
      resp = self._api.update_job(
          job_config, health_check_interval_seconds=health_check_interval_seconds)
    else:
      log.info('No active tasks found for job %s. Creating new job' % job_key)
      resp = self._api.create_job(job_config)

    self._packer.set_live(job_key.role, self._config_package_name(job_key), str(config.version_id))
    return resp

  def reset(self, job_key, version_id, proxy_host):
    log.info("Resetting deployment for job %s to version %s", job_key, version_id)

    config, content = self._fetch_full_config(job_key, str(version_id), proxy_host)
    with NamedTemporaryFile(prefix='job_configuration_') as f:
      f.write(json.dumps(content))
      f.flush()
      self._packer.add(job_key.role, self._config_package_name(job_key), f.name, config.metadata)

  def show(self, job_key, version_id, proxy_host):
    return self._fetch_full_config(job_key, version_id, proxy_host)

  def _fetch_full_config(self, job_key, version_id, proxy_host):
    pkg_name = self._config_package_name(job_key)
    try:
      latest = self._packer.get_version(job_key.role, self._config_package_name(job_key), 'latest')
      version = self._packer.get_version(
          job_key.role, self._config_package_name(job_key), version_id)
    except Packer.Error as e:
      self._handle_packer_error(e, job_key, version_id)

    config = DeploymentConfig.from_packer_get(version, version['id'] == latest['id'])
    with NamedTemporaryFile(prefix='job_configuration_') as job_fp:
      self._packer.fetch(job_key.role, pkg_name, str(version_id), proxy_host, job_fp)
      job_fp.seek(0)
      content = json.loads(job_fp.read())
    return config, content

  def _handle_packer_error(self, e, job_key, version=None):
    # TODO(atollenaere): This is a hack around the fact the python packer client does not
    #                    differentiate between 'no such package' and 'no such version'. We should
    #                    revisit when implementing packer fetch in the packer client.
    #                    see AWESOME-4623
    if 'Requested package or version not found' in str(e):
      if version is None or version == 'latest':
        raise self.NoDeploymentError(job_key)
      try:
        self._packer.list_versions(job_key.role, self._config_package_name(job_key))
        raise self.NoSuchVersion(job_key, version)
      except Packer.Error as e:
        raise self.NoDeploymentError(job_key)
    else:
      raise e


class DeploymentConfig(object):
  """Helper structure to work with deployments"""

  @classmethod
  def from_packer_list(cls, packer_list_output, latest_id):
    """Returns a list of DeploymentConfig from a packer list_versions json structure"""
    return [cls.from_packer_get(version, version['id'] == latest_id)
            for version in packer_list_output]

  @classmethod
  def from_packer_get(cls, packer_get_output, latest):
    """Returns a DeploymentConfig from a packer get_version json structure"""
    return cls(
        packer_get_output['id'],
        packer_get_output['md5sum'],
        packer_get_output['auditLog'],
        packer_get_output['metadata'] or '',  # Metadata can be None
        latest)

  def __init__(self, version_id, md5, auditlog, metadata, latest):
    self.version_id = version_id
    self.md5 = md5
    self.auditlog = auditlog
    self.metadata = metadata
    self.message = ''
    self.latest = latest
    try:
      loaded_metadata = json.loads(metadata)
      if isinstance(loaded_metadata, dict):
        self.message = loaded_metadata.get('message', '')
      else:
        raise ValueError
    except ValueError:
      log.warning('Unexpected value in deployment metadata: %s' % metadata)

  def creation(self):
    """Returns the creation event"""
    return self.auditlog[0]

  def releases(self):
    """List of release events for this deployment"""
    return [log for log in self.auditlog if log['state'] == 'LIVE']

  def released(self):
    """Return True iff this deployment is currently released"""
    return self.auditlog[-1]['state'] == 'LIVE'
