from abc import ABCMeta, abstractproperty
import os
import posixpath
import subprocess
import sys

from twitter.common.git import branch
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_mkdir
from twitter.mesos.clusters import Cluster
from twitter.vert.release import EnvRelease

import git


class Deployer(object):
  class CannotIssueRemotes(Exception): pass

  def __init__(self, remote_user=None, dry_run=True, verbose=False):
    self._dry_run = dry_run
    self._verbose = verbose
    self._remote_user = remote_user

  def check_call(self, cmd):
    """Wrapper for subprocess.check_call."""
    return self.maybe_run_command(subprocess.check_call, cmd)

  def remote_check_call(self, host, cmd):
    return self.check_output(['ssh', self.ssh_target(host)] + cmd)

  def check_output(self, cmd):
    """Stand-in for subprocess.check_output, added in python 2.7"""
    result = self.run_cmd(cmd)
    if result is not None:
      returncode, output = result
      assert returncode == 0, 'Command failed: "%s", output %s' % (' '.join(cmd), output)
      return output

  def cmd_output(self, cmd):
    """Runs a command and returns only its stdout"""
    result = self.run_cmd(cmd)
    if result:
      _, output = result
      return output[0].strip()
    else:
      return None

  def remote_call(self, host, cmd):
    return self.run_cmd(['ssh', self.ssh_target(host)] + cmd)

  def ssh_target(self, host):
    if self._remote_user is None:
      raise Deployer.CannotIssueRemotes('Attempted remote operation, must specify remote_user!')
    return '%s@%s' % (self._remote_user, host)

  def run_cmd(self, cmd):
    """Runs a command and returns its return code along with stderr/stdout tuple"""
    def fork_join(args):
      proc = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
      output = proc.communicate()
      return proc.returncode, output
    return self.maybe_run_command(fork_join, cmd)

  def maybe_run_command(self, runner, cmd):
    if self._verbose or self._dry_run:
      print('%s command: %s' % ('Would run' if self._dry_run else 'Executing', ' '.join(cmd)))
      return 0, ''
    if not self._dry_run:
      return runner(cmd)


class Builder(object):
  __metaclass__ = ABCMeta

  class Error(Exception): pass
  class InvalidDeployTag(Error): pass
  class ReleaseNotFound(Error): pass
  class DirtyRepositoryError(Error): pass

  def __init__(self, cluster, release=None, hotfix=None, verbose=False):
    assert not (release is not None and hotfix), 'Cannot specify both release and hotfix.'
    self._release = self._get_release(cluster, release) if not hotfix else None
    self._cluster = Cluster.get(cluster)
    self._hotfix = bool(hotfix)
    self._verbose = verbose
    self._sha = self._check_tag()

  @property
  def sha(self):
    return self._sha

  @abstractproperty
  def project(self):
    pass

  @classmethod
  def call(cls, cmd, pipe=True):
    target = subprocess.PIPE if pipe else None
    po = subprocess.Popen(cmd, shell=True, stdout=target, stderr=target)
    so, se = po.communicate()
    return po.returncode, so, se

  @classmethod
  def check_call(cls, cmd, *args, **kw):
    rc, so, se = cls.call(cmd, *args, **kw)
    if rc != 0:
      for line in so.splitlines():
        print('stdout: %s' % line)
      for line in se.splitlines():
        print('stderr: %s' % line)
      sys.exit(1)

  def _get_release(self, cluster, release=None):
    if release is None:
      env_release = EnvRelease.latest(self.project, cluster)
      if env_release is None:
        raise self.ReleaseNotFound('Could not find latest release of %s!' % cluster)
    else:
      env_releases = filter(lambda rel: rel.number == release,
                            EnvRelease.list(self.project, cluster))
      if len(env_releases) != 1:
        raise self.ReleaseNotFound('Could not find environment release of release %s' % release)
      env_release = env_releases[0]
    return env_release

  def _check_tag(self):
    """
      Checks that the given tag is valid on origin to enable repeatable builds and returns the sha
      the tag points to.
    """
    repo = git.Repo()
    if repo.is_dirty():
      raise self.DirtyRepositoryError('Cannot run deploy off dirty repository!')

    if self._hotfix:
      return repo.head.commit.hexsha

    failed, _, _ = self.call('git ls-remote --exit-code --tags origin %s' % self._release.tag())
    if failed:
      raise self.InvalidDeployTag('The tag %s was not found on origin' % self._release.tag())

    return self._release.get_commit()

  @property
  def test_commands(self):
    """Returns a list of test commands that must pass prior to building."""
    return []

  @property
  def commands(self):
    """Returns a list of commands in order to build the artifacts associated with this release."""
    return []

  @property
  def artifacts(self):
    """Returns a map of local artifact to remote artifact using $dc and $cluster substitutions."""
    return {}

  def preprocess(self):
    pass

  def postprocess(self):
    pass

  def build(self):
    with branch(self.sha, self.project):
      self.preprocess()
      for test_command in self.test_commands:
        print('Executing test command: %s' % test_command)
        self.check_call(test_command)
      for build_command in self.commands:
        print('Executing build command: %s' % build_command)
        self.check_call(build_command)
      self.postprocess()


class HDFSDeployer(object):
  REMOTE_USER = 'mesos'
  STAGE_HOST = 'nest2.corp.twitter.com'
  STAGE_DIR = '~/release_staging'
  DC_WILDCARD = '$dc'
  CLUSTER_WILDCARD = '$cluster'
  HDFS_BIN_DIR = '/mesos/pkg/mesos/bin'

  def __init__(self, cluster, dry_run=True, verbose=False):
    self._really_deploy = not dry_run
    self._verbose = verbose
    self._deployer = Deployer(self.REMOTE_USER, dry_run, verbose)
    self._cluster_name = cluster
    self._cluster = Cluster.get(cluster)

  def hadoop(self):
    return ['hadoop', '--config', self._cluster.hadoop_config, 'fs']

  def replace_hdfs_file(self, local_file, hdfs_path):
    self._deployer.remote_call(self.STAGE_HOST, self.hadoop() +
        ['-mkdir', posixpath.dirname(hdfs_path)])
    self._deployer.remote_call(self.STAGE_HOST, self.hadoop() + ['-rm', hdfs_path])
    self._deployer.remote_check_call(self.STAGE_HOST, self.hadoop() +
        ['-put', local_file, hdfs_path])

  def send_file_to_hdfs(self, local_path, remote_path):
    self._deployer.remote_check_call(self.STAGE_HOST, ['mkdir', '-p', self.STAGE_DIR])
    stage_file = posixpath.join(self.STAGE_DIR, posixpath.basename(local_path))
    self._deployer.check_output(['scp', local_path, '%s:%s' % (
        self._deployer.ssh_target(self.STAGE_HOST), stage_file)])
    self.replace_hdfs_file(stage_file, remote_path)

  def copy_file_from_hdfs(self, remote_path, local_path):
    if self._really_deploy:
      stdout, _ = self._deployer.remote_check_call(self.STAGE_HOST,
          'mktemp -d /tmp/hdfs.XXXXXX'.split())
    else:
      stdout = '/tmp/hdfs.ABCDEFG'
    stage_dir = stdout.rstrip()
    stage_file = posixpath.join(stage_dir, posixpath.basename(local_path))
    self._deployer.remote_check_call(self.STAGE_HOST, self.hadoop() +
        ['-get', remote_path, stage_file])
    if self._really_deploy:
      safe_mkdir(os.path.dirname(local_path))
    else:
      print 'Would make staging directory: %s' % os.path.dirname(local_path)
    self._deployer.check_output(['scp',
      '%s:%s' % (self._deployer.ssh_target(self.STAGE_HOST), stage_file),
      local_path])
    self._deployer.remote_check_call(self.STAGE_HOST, ['rm', '-rf', stage_dir])

  def hdfs_file_exists(self, remote_path):
    rv, _ = self._deployer.remote_call(self.STAGE_HOST, self.hadoop() +
        ['-test', '-e', remote_path])
    return rv == 0

  def iterate_over_artifact_map(self, builder, root='.'):
    wildcards = {
      self.DC_WILDCARD: self._cluster.dc,
      self.CLUSTER_WILDCARD: self._cluster.local_name,
    }
    for local_file, hdfs_target in builder.artifacts.items():
      hdfs_target = posixpath.join(self.HDFS_BIN_DIR, hdfs_target)
      for wildcard, value in wildcards.items():
        local_file = local_file.replace(wildcard, value)
        hdfs_target = hdfs_target.replace(wildcard, value)
      yield (os.path.join(root, local_file), hdfs_target)

  def stage(self, builder, root='.'):
    for local_file, hdfs_target in self.iterate_over_artifact_map(builder, root):
      if os.path.exists(local_file):
        self.send_file_to_hdfs(local_file, hdfs_target)
      else:
        print('Warning: %s does not exist.' % local_file)

  def checkpoint(self, builder):
    """Checkpoint the existing artifacts into a temporary directory."""
    with temporary_dir(cleanup=False) as temp_root:
      for local_file, hdfs_target in self.iterate_over_artifact_map(builder, temp_root):
        if self.hdfs_file_exists(hdfs_target):
          self.copy_file_from_hdfs(hdfs_target, local_file)
        else:
          print('Warning: %s does not exist.  Perhaps a deploy has never been done?' % hdfs_target)
      return temp_root
