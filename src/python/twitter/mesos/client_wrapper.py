import getpass
import os
import subprocess
import time

from twitter.common import log, dirutil
from twitter.mesos.clusters import Cluster
from twitter.mesos.location import Location

from twitter.mesos.scheduler_client import SchedulerClient
from twitter.mesos.session_key_helper import SessionKeyHelper
from twitter.mesos.tunnel_helper import TunnelHelper
from twitter.mesos.updater import Updater

from gen.twitter.mesos.ttypes import *

class HDFSHelper(object):
  """Helper class for performing HDFS operations."""

  @staticmethod
  def hadoop_uri(cluster):
    """Returns hadoop uri of the cluster."""
    return Cluster.get(cluster).hadoop_uri

  @staticmethod
  def hadoop_config(cluster):
    """Returns hadoop config of the cluster."""
    return Cluster.get(cluster).hadoop_config

  @staticmethod
  def ssh_proxy(cluster):
    """"Returns the ssh proxy to be used with the cluster."""
    return TunnelHelper.get_tunnel_host(cluster) if Location.is_corp() else None

  @staticmethod
  def hdfs_uri(cluster, path):
    """Returns the hdfs uri of the given path."""
    uri = HDFSHelper.hadoop_uri(cluster)
    return "%s%s" % (uri, path)

  @staticmethod
  def fs_call(user, cluster, cmd, *args):
    """Runs hadoop fs command (via proxy if necessary) with the given command and args."""

    log.info("Running hadoop fs %s %s" % (cmd, list(args)))

    hadoop_config = HDFSHelper.hadoop_config(cluster)
    ssh_proxy_host = HDFSHelper.ssh_proxy(cluster)

    if ssh_proxy_host:
      log.info("Running in corp, hadoop fs will be run via %s@%s" % (user, ssh_proxy_host))

    return MesosHelper.call(['hadoop', '--config', hadoop_config, 'fs', cmd] + list(args),
                            ssh_proxy_host, user=user)

  @staticmethod
  def copy_from_hdfs(user, cluster, src, dst):
    """
    Copy file(s) in hdfs to local path (via proxy if necessary).
    NOTE: If src matches multiple files, make sure dst is a directory!
    """
    hdfs_src = HDFSHelper.hdfs_uri(cluster, src)

    log.info('Copying %s -> %s' % (hdfs_src, dst))

    ssh_proxy_host = HDFSHelper.ssh_proxy(cluster)

    if ssh_proxy_host: # copy via a scratch dir on the proxy host.
      scratch_dir = MesosHelper.check_output(['mktemp -d'], ssh_proxy_host)

      HDFSHelper.fs_call(user, cluster, '-get', hdfs_src, scratch_dir)

      log.info("Copying from %s:%s/%s to %s" % (user, ssh_proxy_host, scratch_dir, dst))

      # NOTE: We use call instead of check_call because the scratch directory might be empty!
      subprocess.call(['scp', '-rq', '%s@%s:%s/*' % (user, ssh_proxy_host, scratch_dir), dst])

      MesosHelper.call(['rm -rf %s' % scratch_dir], ssh_proxy_host) # delete the scratch dir
      dirutil.safe_rmtree(scratch_dir) # delete the scratch dir
    else:
      HDFSHelper.fs_call(user, cluster, '-get', hdfs_src, dst)

  @staticmethod
  def copy_to_hdfs(user, cluster, src, dst):
    """
    Copy the local file src to a hadoop path dst. Should be used in
    conjunction with starting new mesos jobs
    """
    abs_src = os.path.expanduser(src)

    dst = HDFSHelper.hdfs_uri(cluster, dst)
    dst_dir = os.path.dirname(dst)

    log.info('Dst: %s Dir: %s' % (dst, dst_dir))

    assert os.path.exists(abs_src), 'App file does not exist, cannot continue - %s' % abs_src

    hdfs_src = abs_src if Location.is_prod() else os.path.basename(abs_src)

    ssh_proxy_host = HDFSHelper.ssh_proxy(cluster)
    hadoop_fs_config = HDFSHelper.hadoop_config(cluster)

    hadoop_fs = ['hadoop', '--config', hadoop_fs_config, 'fs']
    def call_hadoop(*args):
      return MesosHelper.call(hadoop_fs + list(args), ssh_proxy_host, user=user)
    def check_call_hadoop(*args):
      MesosHelper.check_call(hadoop_fs + list(args), ssh_proxy_host, user=user)

    if ssh_proxy_host:
      log.info('Running in corp, copy will be done via %s@%s' % (user, ssh_proxy_host))
      subprocess.check_call(['scp', abs_src, '%s@%s:' % (user, ssh_proxy_host)])
    if not call_hadoop('-test', '-e', dst):
      log.info("Deleting existing file at %s" % dst)
      check_call_hadoop('-rm', '-skipTrash', dst)
    elif call_hadoop('-test', '-e', dst_dir):
      log.info('Creating directory %s' % dst_dir)
      check_call_hadoop('-mkdir', dst_dir)
    log.info('Copying %s -> %s' % (hdfs_src, dst))
    check_call_hadoop('-put', hdfs_src, dst)


class MesosHelper(object):
  _DEFAULT_USER = getpass.getuser()

  @staticmethod
  def acquire_session_key(owner):
    key = SessionKey(user=owner)
    try:
      SessionKeyHelper.sign_session(key, owner)
    except Exception as e:
      log.warning('Cannot use SSH auth: %s' % e)
      log.warning('Attempting un-authenticated communication')
      key.nonce = SessionKeyHelper.get_timestamp()
      key.nonceSig = 'UNAUTHENTICATED'
    return key

  @staticmethod
  def call(cmd, host, user=_DEFAULT_USER):
    cmd = MesosHelper._maybe_tunnel_cmd(cmd, host, user)
    return subprocess.call(cmd)

  @staticmethod
  def check_call(cmd, host, user=_DEFAULT_USER):
    cmd = MesosHelper._maybe_tunnel_cmd(cmd, host, user)
    return subprocess.check_call(cmd)

  @staticmethod
  def check_output(cmd, host, user=_DEFAULT_USER):
    cmd = MesosHelper._maybe_tunnel_cmd(cmd, host, user)
    return subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0].rstrip('\n')

  @staticmethod
  def _maybe_tunnel_cmd(cmd, host, user):
    return ['ssh', '-t', '%s@%s' % (user, host), ' '.join(cmd)] if host is not None else cmd

  @staticmethod
  def assert_valid_cluster(cluster):
    assert cluster, "Cluster not specified!"
    if cluster.find(':') > -1:
      scluster = cluster.split(':')

      if scluster[0] != 'localhost':
        Cluster.assert_exists(scluster[0])

      if len(scluster) == 2:
        try:
          int(scluster[1])
        except ValueError as e:
          log.fatal('The cluster argument is invalid: %s (error: %s)' % (cluster, e))
          assert False, 'Invalid cluster argument: %s' % cluster
    else:
      Cluster.assert_exists(cluster)


class MesosClientBase(object):
  """
  This class is responsible for creating a thrift client
  to the twitter scheduler. Basically all the dirty work
  needed by the MesosClientAPI.
  """

  def __init__(self, cluster, verbose=False):
    self._cluster = cluster
    self._session_key = self._client = self._proxy = self._scheduler = None
    self.verbose = verbose
    MesosHelper.assert_valid_cluster(cluster)

  def with_scheduler(method):
    """Decorator magic to make sure a connection is made to the scheduler"""
    def _wrapper(self, *args, **kwargs):
      if not self._scheduler:
        self._construct_scheduler()
      return method(self, *args, **kwargs)
    return _wrapper

  def requires_auth(method):
    def _wrapper(self, *args, **kwargs):
      if not self._session_key:
        self._session_key = MesosHelper.acquire_session_key(getpass.getuser())
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

  @requires_auth
  def session_key(self):
    return self._session_key

  @with_scheduler
  def scheduler(self):
    return self._scheduler

  def _construct_scheduler(self):
    """
      Populates:
        self._proxy (if proxy necessary)
        self._scheduler
        self._client
    """
    self._proxy, self._scheduler = SchedulerClient.get(self._cluster, verbose=self.verbose)
    assert self._scheduler, "Could not find scheduler (cluster = %s)" % self._cluster
    self._client = self._scheduler.get_thrift_client()
    assert self._client, "Could not construct thrift client."


class MesosClientAPI(MesosClientBase):
  """This class provides the API to talk to the twitter scheduler"""

  def __init__(self, **kwargs):
    super(MesosClientAPI, self).__init__(**kwargs)

  def hdfs_path(self, config, copy_app_from):
    if config.hdfs_path():
      return config.hdfs_path()
    else:
      return '/mesos/pkg/%s/%s' % (config.role(), os.path.basename(copy_app_from))

  def create_job(self, config, copy_app_from=None):
    if copy_app_from is not None:
      HDFSHelper.copy_to_hdfs(config.role(), self.cluster(),
                              copy_app_from, self.hdfs_path(config, copy_app_from))

    log.info('Creating job %s' % config.name())
    return self.client().createJob(config.job(), self.session_key())

  def start_cronjob(self, role, jobname):
    log.info("Starting cron job: %s" % jobname)

    return self.client().startCronJob(role, jobname, self.session_key())

  def kill_job(self, role, jobname):
    log.info("Killing tasks for job: %s" % jobname)

    query = TaskQuery()
    query.owner = Identity(role=role)
    query.jobName = jobname

    return self.client().killTasks(query, self.session_key())

  def check_status(self, role, jobname=None):
    log.info("Checking status of role: %s / job: %s" % (role, jobname))

    query = TaskQuery()
    query.owner = Identity(role=role)
    if jobname:
      query.jobName = jobname
    return self.client().getTasksStatus(query)

  def update_job(self, config, copy_app_from=None):
    log.info("Updating job: %s" % config.name())

    if copy_app_from is not None:
      HDFSHelper.copy_to_hdfs(config.role(), self.cluster(),
                              copy_app_from, self.hdfs_path(config, copy_app_from))

    resp = self.client().startUpdate(config.job(), self.session_key())

    if resp.responseCode != ResponseCode.OK:
      log.info("Error doing start update: %s" % resp.message)

      # Create a update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = ResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    # TODO(William Farner): Cleanly handle connection failures in case the scheduler
    #                       restarts mid-update.
    updater = Updater(config.role(), config.name(), self.client(), time, resp.updateToken,
                      self.session_key())
    failed_shards = updater.update(
      config.update_config(), sorted(map(lambda task: task.shardId, config.job().taskConfigs)))

    if failed_shards:
      log.info('Update reverted, failures detected on shards %s' % failed_shards)
    else:
      log.info('Update Successful')

    resp = self.client().finishUpdate(
      config.role(), config.name(), UpdateResult.FAILED if failed_shards else UpdateResult.SUCCESS,
      resp.updateToken, self._session_key)

    if resp.responseCode != ResponseCode.OK:
      log.info("Error doing finish update: %s" % resp.message)

      # Create a update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = ResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    resp = FinishUpdateResponse()
    resp.responseCode = ResponseCode.WARNING if failed_shards else ResponseCode.OK
    resp.message = "Update Unsuccessful" if failed_shards else "Update Successful"
    return resp

  def cancel_update(self, role, jobname):
    log.info("Canceling update on job: %s" % jobname)

    resp = self.client().finishUpdate(role, jobname, UpdateResult.TERMINATE,
        None, self.session_key())

    if resp.responseCode != ResponseCode.OK:
      log.info("Error cancelling the update: %s" % resp.message)

      # Create a update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = ResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    resp = FinishUpdateResponse()
    resp.responseCode = ResponseCode.OK
    resp.message = "Update Cancelled"
    return resp

  def get_quota(self, role):
    log.info("Getting quota for: %s" % role)

    return self.client().getQuota(role)

  def set_quota(self, role, cpu, ram_mb, disk_mb):
    log.info("Setting quota for user:%s cpu:%f ram_mb:%d disk_mb: %d"
              % (role, cpu, ram_mb, disk_mb))

    return self.client().setQuota(role, Quota(cpu, ram_mb, disk_mb), self.session_key())

  def force_task_state(self, task_id, status):
    log.info("Requesting that task %s transition to state %s" % (task_id, status))
    return self.client().forceTaskState(task_id, status, self.session_key())
