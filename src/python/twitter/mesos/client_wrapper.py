import getpass
import os
import pprint
import subprocess
import time

from twitter.common import log
from twitter.mesos import clusters
from twitter.mesos.location import Location
from twitter.mesos.proxy_config import ProxyConfig
from twitter.mesos.scheduler_client import LocalSchedulerClient, ZookeeperSchedulerClient
from twitter.mesos.session_key_helper import SessionKeyHelper
from twitter.mesos.tunnel_helper import TunnelHelper
from twitter.mesos.updater import Updater

from gen.twitter.mesos.ttypes import *

class MesosHelper(object):
  _DEFAULT_USER = getpass.getuser()

  @staticmethod
  def acquire_session_key_or_die(owner):
    key = SessionKey(user=owner)
    SessionKeyHelper.sign_session(key, owner)
    return key

  @staticmethod
  def is_admin():
    try:
      MesosHelper.acquire_session_key_or_die('mesos')
    except SessionKeyHelper.AuthorizationError:
      return False
    return True

  @staticmethod
  def call(cmd, host, user=_DEFAULT_USER):
    if host is not None:
      return subprocess.call(['ssh', '%s@%s' % (user, host), ' '.join(cmd)])
    else:
      return subprocess.call(cmd)

  @staticmethod
  def check_call(cmd, host, user=_DEFAULT_USER):
    if host is not None:
      return subprocess.check_call(['ssh', '%s@%s' % (user, host), ' '.join(cmd)])
    else:
      return subprocess.check_call(cmd)

  @staticmethod
  def assert_valid_cluster(cluster):
    assert cluster, "Cluster not specified!"
    if cluster.find(':') > -1:
      scluster = cluster.split(':')

      if scluster[0] != 'localhost':
        clusters.assert_exists(scluster[0])

      if len(scluster) == 2:
        try:
          int(scluster[1])
        except ValueError as e:
          log.fatal('The cluster argument is invalid: %s (error: %s)' % (cluster, e))
          assert False, 'Invalid cluster argument: %s' % cluster
    else:
      clusters.assert_exists(cluster)

  @staticmethod
  def get_cluster(cmd_line_arg_cluster, config_cluster):
    if config_cluster and cmd_line_arg_cluster and config_cluster != cmd_line_arg_cluster:
      log.error(
      """Warning: --cluster and the cluster in your configuration do not match.
         Using the cluster specified on the command line. (cluster = %s)""" % cmd_line_arg_cluster)
    cluster = cmd_line_arg_cluster or config_cluster
    MesosHelper.assert_valid_cluster(cluster)
    return cluster

  @staticmethod
  def copy_to_hadoop(user, ssh_proxy_host, src, dst):
    """
    Copy the local file src to a hadoop path dst. Should be used in
    conjunction with starting new mesos jobs
    """
    abs_src = os.path.expanduser(src)
    dst_dir = os.path.dirname(dst)

    assert os.path.exists(abs_src), 'App file does not exist, cannot continue - %s' % abs_src

    hdfs_src = abs_src if Location.is_prod() else os.path.basename(abs_src)

    if ssh_proxy_host:
      log.info('Running in corp, copy will be done via %s@%s' % (user, ssh_proxy_host))
      subprocess.check_call(['scp', abs_src, '%s@%s:' % (user, ssh_proxy_host)])

    if not MesosHelper.call(['hadoop', 'fs', '-test', '-e', dst], ssh_proxy_host, user=user):
      log.info("Deleting existing file at %s" % dst)
      MesosHelper.check_call(['hadoop', 'fs', '-rm', dst], ssh_proxy_host, user=user)
    elif MesosHelper.call(['hadoop', 'fs', '-test', '-e', dst_dir], ssh_proxy_host, user=user):
      log.info('Creating directory %s' % dst_dir)
      MesosHelper.check_call(['hadoop', 'fs', '-mkdir', dst_dir], ssh_proxy_host, user=user)

    log.info('Copying %s -> %s' % (hdfs_src, dst))
    MesosHelper.check_call(['hadoop', 'fs', '-put', hdfs_src, dst], ssh_proxy_host, user=user)

  @staticmethod
  def copy_app_to_hadoop(user, source_path, hdfs_path, cluster, ssh_proxy):
    assert hdfs_path is not None, 'No target HDFS path specified'
    hdfs = clusters.get_hadoop_uri(cluster)
    hdfs_uri = '%s%s' % (hdfs, hdfs_path)
    MesosHelper.copy_to_hadoop(user, ssh_proxy, source_path, hdfs_uri)

  @staticmethod
  def get_config(jobname, config_file, new_mesos):
    """Returns the proxy config."""
    if new_mesos:
      assert MesosHelper.is_admin(), ("--new_mesos is currently only allowed"
                                      " for users in the mesos group.")

      config = ProxyConfig.from_new_mesos(config_file)
    else:
      config = ProxyConfig.from_mesos(config_file)

    config.set_job(jobname)

    return config


class MesosClientBase(object):
  """
  This class is responsible for creating a thrift client
  to the twitter scheduler. Basically all the dirty work
  needed by the MesosClientAPI.
  """

  def __init__(self, cluster=None, verbose=False):
    self._cluster = self._config = self._client = self._proxy = self._scheduler = None
    self.force_cluster = cluster
    self.verbose = verbose

  def with_scheduler(method):
    """Decorator magic to make sure a connection is made to the scheduler"""
    def _wrapper(self, *args, **kwargs):
      if not self._scheduler:
        self._construct_scheduler(self._config)
      return method(self, *args, **kwargs)
    return _wrapper

  @with_scheduler
  def client(self):
    return self._client

  @with_scheduler
  def proxy(self):
    return self._proxy

  @with_scheduler
  def cluster(self):
    return self._cluster

  def config(self):
    assert self._config, 'Config is unset, have you called get_config?'
    return self._config

  def _tunnel_user(self, job, tunnel_as):
    return tunnel_as or job.owner.role

  @staticmethod
  def get_scheduler_client(cluster, **kwargs):
    log.info('Auto-detected location: %s' % 'prod' if Location.is_prod() else 'corp')

    # TODO(vinod) : Clear up the convention of what --cluster <arg> means
    # Currently arg can be any one of
    # 'localhost:<scheduler port>'
    # '<cluster name>'
    # '<cluster name>: <zk port>'
    # Instead of encoding zk port inside the <arg> string make it explicit.
    if cluster.find('localhost:') == 0:
      log.info('Attempting to talk to local scheduler.')
      port = int(cluster.split(':')[1])
      return None, LocalSchedulerClient(port, ssl=True)
    else:
      if cluster.find(':') > -1:
        cluster, zk_port = cluster.split(':')
        zk_port = int(zk_port)
      else:
        zk_port = 2181

      force_notunnel = True

      if cluster == 'angrybird-local':
        ssh_proxy_host = None
      else:
        ssh_proxy_host = TunnelHelper.get_tunnel_host(cluster) if Location.is_corp() else None

      if ssh_proxy_host is not None:
        log.info('Proxying through %s' % ssh_proxy_host)
        force_notunnel = False

      return ssh_proxy_host, ZookeeperSchedulerClient(cluster, zk_port,
                                                      force_notunnel, ssl=True, **kwargs)

  def _construct_scheduler(self, config=None):
    """
      Populates:
        self._cluster
        self._proxy (if proxy necessary)
        self._scheduler
        self._client
    """
    self._cluster = MesosHelper.get_cluster(
      self.force_cluster, config.cluster() if config else None)
    self._proxy, self._scheduler = self.get_scheduler_client(
      self._cluster, verbose=self.verbose)
    assert self._scheduler, "Could not find scheduler (cluster = %s)" % self._cluster
    self._client = self._scheduler.get_thrift_client()
    assert self._client, "Could not construct thrift client."

  def acquire_session(self):
    return MesosHelper.acquire_session_key_or_die(getpass.getuser())


class MesosClientAPI(MesosClientBase):
  """This class provides the API to talk to the twitter scheduler"""

  def __init__(self, **kwargs):
    super(MesosClientAPI, self).__init__(**kwargs)

  def create_job(self, jobname, config, copy_app_from=None, tunnel_as=None):
    sessionkey = self.acquire_session()

    job = config.job(jobname)
    self._config = config

    if copy_app_from is not None:
      MesosHelper.copy_app_to_hadoop(self._tunnel_user(job, tunnel_as), copy_app_from,
          self.config().hdfs_path(), self.cluster(), self.proxy())

    log.info('Creating job %s' % job.name)
    resp = self.client().createJob(job, sessionkey)
    return resp

  def inspect(self, jobname, config, copy_app_from=None):
    if copy_app_from is not None:
      if self.config().hdfs_path():
        log.info('Detected HDFS package: %s' % self.config().hdfs_path())
        log.info('Would copy to %s via %s.' % (self.cluster(), self.proxy()))

    pprint.pprint(config.job(jobname))

    sessionkey = self.acquire_session()
    log.info('Parsed job: %s' % jobname)

  def start_cronjob(self, role, jobname):
    log.info("Starting cron job: %s" % jobname)

    resp = self.client().startCronJob(role, jobname, self.acquire_session())
    return resp

  def kill_job(self, role, jobname):
    log.info("Killing tasks for job: %s" % jobname)

    query = TaskQuery()
    query.owner = Identity(role=role)
    query.jobName = jobname

    resp = self.client().killTasks(query, self.acquire_session())
    return resp

  def check_status(self, role, jobname):
    log.info("Checking status of job: %s" % jobname)

    query = TaskQuery()
    query.owner = Identity(role=role)
    query.jobName = jobname

    resp = self.client().getTasksStatus(query)
    return resp

  def update_job(self, jobname, config, copy_app_from=None, tunnel_as=None):
    log.info("Updating job: %s" % jobname)

    sessionkey = self.acquire_session()

    job = config.job(jobname)
    self._config = config

    # TODO(William Farner): Add a rudimentary versioning system here so application updates
    #                       don't overwrite original binary.
    if copy_app_from is not None:
      MesosHelper.copy_app_to_hadoop(self._tunnel_user(job, tunnel_as), copy_app_from,
          self.config().hdfs_path(), self.cluster(), self.proxy())

    resp = self.client().startUpdate(job, sessionkey)

    if resp.responseCode != ResponseCode.OK:
      log.info("Error doing start update: %s" % resp.message)

      # Create a update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = UpdateResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    # TODO(William Farner): Cleanly handle connection failures in case the scheduler
    #                       restarts mid-update.
    updater = Updater(job.owner.role, job.name, self.client(), time, resp.updateToken, sessionkey)
    failed_shards = updater.update(job)

    if failed_shards:
      log.info('Update reverted, failures detected on shards %s' % failed_shards)
    else:
      log.info('Update Successful')

    resp = self.client().finishUpdate(
      job.owner.role, job.name, UpdateResult.FAILED if failed_shards else UpdateResult.SUCCESS,
      resp.updateToken, sessionkey)

    return resp

  def cancel_update(self, jobname):
    log.info("Canceling update on job: %s" % jobname)

    resp = self.client().finishUpdate(role, jobname, UpdateResult.TERMINATE,
                                      None, self.acquire_session())
    return resp

  def get_quota(self, role):
    log.info("Getting quota for: %s" % role)

    resp = self.client().getQuota(role)
    return resp

  def set_quota(self, role, cpu, ram_mb, disk_mb):
    log.info("Setting quota for user:%s cpu:%f ram_mb:%d disk_mb: %d"
              % (role, cpu, ram_mb, disk_mb))

    resp = self.client().setQuota(role, Quota(cpu, ram_mb, disk_mb), self.acquire_session())
    return resp
