import json
from operator import itemgetter
import os
import subprocess
import sys

from twitter.common import log
from twitter.common.contextutil import open_zip
from twitter.common.dirutil import Fileset, safe_mkdtemp
from twitter.common.git import branch, git


class CommandLineBuilder(object):

  class Error(Exception): pass

  @classmethod
  def call(cls, cmd):
    log.info('Running: %s' % cmd)
    po = subprocess.Popen(cmd, shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=sys.stdin)
    so, se = po.communicate()
    for line in so.splitlines():
      log.info('stdout: %s' % line)
    for line in se.splitlines():
      log.info('stderr: %s' % line)
    rc = po.returncode
    if rc != 0:
      log.error('Command returned non-zero (%d) exit code: %s' % (rc, cmd))
    return rc, so, se

  @classmethod
  def check_call(cls, cmd, *args, **kw):
    rc, _, _ = cls.call(cmd, *args, **kw)
    if rc != 0:
      raise cls.Error('Command Failed: %s' % cmd)

  def __init__(self, build_spec):
    self.test_command = build_spec.get('test_command')
    self.build_command = build_spec.get('build_command')
    self.artifact_globs = build_spec['artifact_globs']

  @property
  def artifacts(self):
    artifacts = set()
    for glob in self.artifact_globs:
      artifacts.update(Fileset.zglobs(glob)())
    return artifacts

  def run(self):
    for command in (self.test_command, self.build_command):
      if command is not None:
        self.check_call(command)


class GitBuilder(CommandLineBuilder):

  def __init__(self, build_spec):
    super(GitBuilder, self).__init__(build_spec)
    self.gitsha = build_spec['gitsha']
    self._repo = git.Repo(os.getcwd())
    self.resolved_gitsha = self._repo.rev_parse(self.gitsha).hexsha

  def validate_repo(self, require_origin_master=False):
    if self._repo.is_dirty():
      raise self.Error('Can not run the build from a dirty repository!')
    sha = self.resolved_gitsha
    if require_origin_master:
      for commit in self._repo.iter_commits(rev='origin/master'):
        if sha == commit.hexsha:
          break
      else:
        raise self.Error('Non-master shas can not be candidates for releases!')
    return sha

  def run(self):
    sha = self.validate_repo()
    with branch(sha):
      super(GitBuilder, self).run()


class PreBuiltPackage(object):
  def __init__(self, prebuilt_pkg_struct):
    self.raw = prebuilt_pkg_struct
    self.__dict__.update(prebuilt_pkg_struct)
    try:
      metadata = json.loads(prebuilt_pkg_struct.get('metadata', ''))
      self.test_command = metadata['test_command']
      self.build_command = metadata['build_command']
      self.gitsha = metadata['gitsha']
      self.artifact_globs = metadata['artifact_globs']
    except (KeyError, ValueError):
      raise ValueError('Not a package via the prebuilt strategy.')


class BuildArtifactResolver(object):
  """
    A Build Artifact Resolver for built packages
    Resolves a built artifact using build commands and uses Packer for storing the artifacts

    Will make a best-effort attempt to avoid duplicate publishing, by first checking if there
    are any existing packages in the repository with the given git SHA.
    Duplicate packages may still result if no SHA is provided or if multiple builds are
    performed simultaneously by separate clients.
  """

  class Error(Exception): pass
  class BadArtifacts(Error): pass

  BUILD_PKG_PREFIX = '__build_'

  def __init__(self, packer, packer_role):
    self.packer = packer
    self.role = packer_role

  @classmethod
  def build_pkg_name(cls, build_spec_name):
    return cls.BUILD_PKG_PREFIX + build_spec_name

  def _zip_artifacts(self, artifacts, artifact_name):
    artifact_dir = safe_mkdtemp()
    artifacts_zip_name = os.path.join(artifact_dir, "%s.zip" % artifact_name)
    with open_zip(artifacts_zip_name, "w") as artifacts_zip:
      for artifact in artifacts:
        # File will be at the same path when extracted since there is no advice on arcname
        # TODO(sgeorge): Remove all leading common directories? Let customers advise
        # E.g. dist/a.pex will extract to a.pex if all other artifacts begin with dist/
        artifacts_zip.write(artifact)
    return artifacts_zip_name

  def _upload(self, pkg_name, artifact_name, gitsha):
    """
      Upload the artifacts for the given build_spec
    """
    metadata = {
      'gitsha': gitsha,
      'test_command': self.builder.test_command,
      'build_command': self.builder.build_command,
      'artifact_globs': self.builder.artifact_globs,
    }
    artifacts = self.builder.artifacts
    if len(artifacts) == 0:
      raise self.BadArtifacts('No artifacts for artifact_globs: %s' %
          ','.join(metadata['artifact_globs']))
    elif len(artifacts) > 1:
      package_file = self._zip_artifacts(artifacts, artifact_name)
    elif len(artifacts) == 1:
      package_file = list(artifacts)[0]

    return PreBuiltPackage(self.packer.add(self.role, pkg_name, package_file, json.dumps(metadata)))

  def _find_package(self, name):
    """
      Finds latest package with metadata matching given build_spec
    """
    try:
      pkg_versions = self.packer.list_versions(self.role, name)
    except Exception:
      return None

    matches = []
    pkg_versions = sorted(pkg_versions, key=itemgetter('id'))
    for build_pkg_struct in pkg_versions:
      try:
        package = PreBuiltPackage(build_pkg_struct)
      except ValueError:
        continue
      if (package.gitsha == self.builder.resolved_gitsha and
          package.build_command == self.builder.build_command and
          package.test_command == self.builder.test_command and
          set(package.artifact_globs) == set(self.builder.artifact_globs)):
        matches.append(package)
    if len(matches) >= 1:
      # There would be 2 or more matches iff multiple concurrent sessions executed
      # find_packages() and found no matching package for the given build_spec
      # and then proceeded to publish the package binary for the same build_spec
      return matches[-1]
    return None

  def resolve(self, build_spec_name, build_spec):
    """
      Resolve an artifact for the given build_spec. If necessary build and publish
    """
    if 'gitsha' in build_spec:
      return self.resolve_git_build(build_spec_name, build_spec)
    else:
      return self.resolve_nongit_build(build_spec_name, build_spec)

  def resolve_git_build(self, build_spec_name, build_spec):
    self.builder = GitBuilder(build_spec)
    gitsha = self.builder.resolved_gitsha
    pkg_name = self.build_pkg_name(build_spec_name)
    package = self._find_package(pkg_name)
    if not package:
      self.builder.run()
      package = self._upload(pkg_name, build_spec_name, gitsha)
    return pkg_name, package.raw

  def resolve_nongit_build(self, build_spec_name, build_spec):
    self.builder = CommandLineBuilder(build_spec)
    self.builder.run()
    pkg_name = self.build_pkg_name(build_spec_name)
    package = self._upload(pkg_name, build_spec_name, gitsha=None)
    return pkg_name, package.raw
