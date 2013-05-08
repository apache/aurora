from abc import ABCMeta, abstractproperty
import os
import posixpath
import subprocess
import sys

from twitter.common.git import branch
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_mkdir
from twitter.mesos.clusters import Cluster
from twitter.vert.backends.tag import TagBackend

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
    if self._dry_run:
      return 0, ''
    else:
      return runner(cmd)


class Builder(object):
  __metaclass__ = ABCMeta

  class Error(Exception): pass
  class InvalidDeployTag(Error): pass
  class ReleaseNotFound(Error): pass
  class DirtyRepositoryError(Error): pass

  def __init__(self, cluster, release=None, hotfix=None, verbose=False):
    assert not (release is not None and hotfix), 'Cannot specify both release and hotfix.'
    self._deploy = self._get_deploy(cluster, release) if not hotfix else None
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

  def _get_deploy(self, cluster, release=None):
    backend = TagBackend()
    deploy = backend.latest_deploy(self.project, cluster) if release is None else (
        backend.get_deploy(self.project, cluster, release))
    if deploy is None:
      raise self.ReleaseNotFound('Could not find release for %s!' % cluster)
    return deploy

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

    return self._deploy.release.revision

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
