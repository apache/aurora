import json
from operator import itemgetter
import os

from twitter.common import app, log
from twitter.common.contextutil import open_zip
from twitter.common.dirutil import safe_mkdtemp

import jenkinsapi.jenkins


__all__ = [
  'JenkinsArtifactResolver',
]


# TODO(wickman): This option is global i.e. not tailored to relevant sub-commands
app.add_option(
    '--jenkins_url',
    dest='jenkins_url',
    default='http://jvm-ci.twitter.biz',
    help='The jenkins URL to fetch jenkins artifacts.'
)


class JenkinsPackage(object):
  def __init__(self, jenkins_pkg_struct):
    self.raw = jenkins_pkg_struct
    self.__dict__.update(jenkins_pkg_struct)
    try:
      metadata = json.loads(jenkins_pkg_struct.get('metadata', ''))
      self.jenkins_project = metadata['project']
      self.jenkins_build_number = metadata['build_number']
    except (KeyError, ValueError):
      raise ValueError('Not a Jenkins package.')


class JenkinsArtifactResolver(object):
  """A Jenkins Artifact Resolver for packages (backed by Packer)

     It exposes Jenkins artifacts to Mesos configurations, utilising packer under the hood.

     Avoids duplicate package publishing by keeping track of jenkins project name and build number

     WARNING: Doesn't prevent duplicates: Two users running mesos_client could have determined that
           jenkins job 'A' with job number '11' hasn't been published yet, and both will then
           proceed to publish a package for job 'A', number '11'
  """

  class Error(Exception): pass
  class UnknownProject(Error): pass
  class UnknownVersion(Error): pass
  class BadArtifacts(Error): pass

  JENKINS_PKG_PREFIX = '__jenkins_'

  def __init__(self, packer, packer_role):
    self.jenkins = jenkinsapi.jenkins.Jenkins(app.get_options().jenkins_url)
    self.packer = packer
    self.role = packer_role

  def resolve_build(self, project, build_number='latest'):
    """
      Return the jenkinsapi Build instance for the given build number
    """
    try:
      job = self.jenkins.get_job(project)
    except jenkinsapi.jenkins.UnknownJob:
      raise self.UnknownProject('Unknown jenkins project: %s' % project)

    if build_number == 'latest':
      return job.get_last_good_build()

    try:
      build = job.get_build(int(build_number))
    except ValueError:
      raise ValueError('Build number must be an integer, got %r' % build_number)
    except KeyError:
      raise self.UnknownVersion('Build number %d does not exist!' % build_number)

    return build

  @classmethod
  def jenkins_pkg_name(cls, project):
    return cls.JENKINS_PKG_PREFIX + project

  def _download_artifact(self, artifact, artifact_dir, project, build_number):
    log.info('Downloading %s/%d %s' % (project, build_number, artifact.filename))
    artifact.savetodir(artifact_dir)

  def _zip_artifacts(self, artifacts, project, build_number):
    artifact_dir = safe_mkdtemp()
    artifacts_zip_name = os.path.join(artifact_dir, "%s.zip" % project)

    with open_zip(artifacts_zip_name, "w") as artifacts_zip:
      for artifact in artifacts:
        artifact_filename = artifact.filename
        self._download_artifact(artifact, artifact_dir, project, build_number)
        artifacts_zip.write(os.path.join(artifact_dir, artifact_filename), artifact_filename)

    return artifacts_zip_name

  def _single_artifact(self, artifact, project, build_number):
    artifact_dir = safe_mkdtemp()
    artifact_filename = artifact.filename
    self._download_artifact(artifact, artifact_dir, project, build_number)
    return os.path.join(artifact_dir, artifact_filename)

  def upload(self, project, build_number, build):
    """
      Upload the artifacts for the given jenkins project name and build number
    """
    pkg_name = self.jenkins_pkg_name(project)
    metadata = {
      'project': project,
      'build_number': build_number,
    }
    artifacts = list(build.get_artifacts())
    if len(artifacts) == 0:
      raise self.BadArtifacts('No artifacts for %s/%d' % (project, build_number))
    elif len(artifacts) > 1:
      package_file = self._zip_artifacts(artifacts, project, build_number)
    elif len(artifacts) == 1:
      package_file = self._single_artifact(artifacts[0], project, build_number)

    return JenkinsPackage(self.packer.add(self.role, pkg_name, package_file, json.dumps(metadata)))

  def find_package(self, project, build_number):
    """
      Finds latest package with metadata matching given jenkins project name and build number
    """
    pkg_name = self.jenkins_pkg_name(project)
    try:
      pkg_versions = self.packer.list_versions(self.role, pkg_name)
    except Exception:
      return None
    matches = []
    pkg_versions = sorted(pkg_versions, key=itemgetter('id'))
    for jenkins_pkg_struct in pkg_versions:
      try:
        package = JenkinsPackage(jenkins_pkg_struct)
      except ValueError:
        continue
      if package.jenkins_build_number == build_number and package.jenkins_project == project:
        matches.append(package)
    if len(matches) >= 1:
      # There would be 2 or more matches iff multiple concurrent sessions executed
      # find_packages() and found no matching package for the job-name/job-build_number
      # and then proceeded to publish the exact same package binary multiple times
      return matches[-1]
    return None

  def resolve(self, project, build_number='latest'):
    """
      Resolve given jenkins project name and build number to a Packer package
    """
    build_obj = self.resolve_build(project, build_number)
    build_number = build_obj.id()
    package = (self.find_package(project, build_number) or
        self.upload(project, build_number, build_obj))
    return self.jenkins_pkg_name(project), package.raw
