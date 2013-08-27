import json
import os

from twitter.common.contextutil import open_zip

from twitter.aurora.client.jenkins import JenkinsArtifactResolver

from dingus import Dingus, DingusTestCase, patch


# Helper constants for Dingus
DINGUS_FIRST_CALL = 0
DINGUS_POS_ARGS = 1
DINGUS_KW_ARGS = 2

# Helper constants for Packer Add arguments
ARG_ROLE = 0
ARG_PKG = 1
ARG_FILE = 2
ARG_METADATA = 3


class FakeArtifact(object):
  """
    An fake Artifact that emulates artifacts and returns appropriate responses
  """
  def __init__(self, filename, contents):
    self.filename = filename
    self.contents = contents

  def savetodir(self, dirname):
    with open(os.path.join(dirname, self.filename), 'wb') as f:
      f.write(self.contents)


class BaseTest(DingusTestCase(JenkinsArtifactResolver,
                              ['app', 'itemgetter', 'json', 'JenkinsPackage',
                               'log', 'open_zip', 'os', 'safe_mkdtemp'])):
  """
    These tests verify that, when resolving a jenkins-project and build-number,
    1. If build-number is 'latest', then Jenkins is checked to resolve 'latest'
       as the last good build number
    2. Packer is checked to see if build artifacts for that build number
       have already been published
    3. If previously published (mocked), it resolves to the matching package version
    4. If not previously published (mocked), the artifacts are published to packer
    5. if the package was uploaded, its contents are correct
    6. if the package was uploaded, its metadata is correct
    7. resolve() returns appropriate package raw struct

    The test function names describe which case is tested and what is mocked
    The subclasses of this class exercise the details of various cases

    Note: json, operator, os and open_zip are not mocked
  """

  def setup(self):
    super(BaseTest, self).setup()

    self.CLUSTER = 'cluster1'
    self.ROLE = 'some-role'
    self.JENKINS_PROJECT = 'some-jenkins-project'

  @classmethod
  def generate_some_jenkins_packages(cls, jenkins_project, jenkins_build_number,
                                     include_build_number=True, num_pkgs=5):
    pkgs = []
    for i in range(num_pkgs):
      pkgs.append({
        'id': i + 1,
        'uri': 'some-uri',
        'md5sum': 'some-md5sum-{0}'.format(i),
        'metadata': json.dumps({
          'project': jenkins_project,
          'build_number': jenkins_build_number - num_pkgs + i + int(include_build_number)
        })
      })
    return pkgs

  @classmethod
  def generate_specific_jenkins_package(cls, jenkins_project, jenkins_build_number):
    return cls.generate_some_jenkins_packages(jenkins_project, jenkins_build_number, num_pkgs=1)[0]

  def setup_default_mocks(self,
                          jenkins_build_number='latest',
                          previously_published=False,
                          num_artifacts=1,
                          latest_build_number=100):
    """
      This sets up the following:
      * Some artifact mocks
      * Connects artifact mocks to Build Mock
      * Connects Build Mock to Jenkins Job Mock
      * Connects Jenkins Job Mock to Jenkins Mock
      * Mocks up Packer
    """

    self.NUM_ARTIFACTS = num_artifacts
    self.PREVIOUSLY_PUBLISHED = previously_published
    self.JENKINS_BUILD_NUMBER = jenkins_build_number
    self.LATEST_BUILD_NUMBER = latest_build_number
    if self.JENKINS_BUILD_NUMBER == 'latest':
      self.REAL_BUILD_NUMBER = self.LATEST_BUILD_NUMBER
    else:
      self.REAL_BUILD_NUMBER = int(self.JENKINS_BUILD_NUMBER)
    self.JENKINS_PKG_NAME = JenkinsArtifactResolver.jenkins_pkg_name(self.JENKINS_PROJECT)

    # Mock Packer: Program expected method return values
    self.mocked_pkg_versions = self.generate_some_jenkins_packages(
        self.JENKINS_PROJECT,
        self.REAL_BUILD_NUMBER,
        include_build_number=self.PREVIOUSLY_PUBLISHED
    )
    self.mock_packer = Dingus(
      list_versions__returns=self.mocked_pkg_versions,
      add__returns=self.generate_specific_jenkins_package(
          self.JENKINS_PROJECT,
          self.REAL_BUILD_NUMBER,
      ),
    )

    # Mock Jenkins and Job
    with patch('jenkinsapi.jenkins.Jenkins', Dingus()):
      self.jenkins_artifact_resolver = JenkinsArtifactResolver(self.mock_packer, self.ROLE)

    self.mock_jenkins = self.jenkins_artifact_resolver.jenkins

    self.mock_job = self.mock_jenkins.get_job.return_value

    # Mock Artifacts
    self.mock_artifacts = []
    for i in range(self.NUM_ARTIFACTS):
      self.mock_artifacts.append(FakeArtifact(
          filename='f%d' % i,
          contents='c%d' % i,
      ))

    # Mock Build
    if self.JENKINS_BUILD_NUMBER == 'latest':
      self.mock_build = self.mock_job.get_last_good_build.return_value
    else:
      self.mock_build = self.mock_job.get_build.return_value

    self.mock_build.id.return_value = self.REAL_BUILD_NUMBER
    self.mock_build.get_artifacts.return_value = self.mock_artifacts

    # Execute common test setup code (exclude if more functions tested)
    self.do_resolve()

  def do_resolve(self):
    self.resolve_return_value = self.jenkins_artifact_resolver.resolve(
        self.JENKINS_PROJECT,
        self.JENKINS_BUILD_NUMBER
    )

  # Tests that are common to all scenarios are here:
  def test_resolve_should_get_jenkins_job(self):
    assert self.mock_jenkins.calls('get_job').once()

  def test_resolve_should_list_versions(self):
    assert self.mock_packer.calls('list_versions').once()

  def test_resolve_should_return_pkg_with_appropriate_metadata(self):
    _, package = self.resolve_return_value
    metadata = json.loads(package['metadata'])
    assert metadata['project'] == self.JENKINS_PROJECT
    assert metadata['build_number'] == self.REAL_BUILD_NUMBER

  # Tests that depend on build_number: 'latest' or specific
  def test_resolve_should_get_appropriate_jenkins_build(self):
    if self.JENKINS_BUILD_NUMBER == 'latest':
      assert self.mock_job.calls('get_last_good_build').once()
    else:
      assert self.mock_job.calls('get_build').once()
    assert self.mock_build.calls('id').once()

  # Tests that depend on if artifact was previously published
  def test_resolve_should_get_jenkins_artifacts(self):
    if self.PREVIOUSLY_PUBLISHED:
      assert len(self.mock_build.calls('get_artifacts')) == 0
    else:
      assert self.mock_build.calls('get_artifacts').once()

  def test_resolve_should_upload_to_packer_if_appropriate(self):
    if self.PREVIOUSLY_PUBLISHED:
      assert len(self.mock_packer.calls('add')) == 0
    else:
      assert self.mock_packer.calls('add').once()
      self.should_upload_correct_package_metadata()
      self.should_upload_correct_package_contents()

  def should_upload_correct_package_metadata(self):
    packer_add_args = self.mock_packer.calls('add')[DINGUS_FIRST_CALL][DINGUS_POS_ARGS]
    assert packer_add_args[ARG_ROLE] == self.ROLE
    assert packer_add_args[ARG_PKG] == self.JENKINS_PKG_NAME
    assert json.loads(packer_add_args[ARG_METADATA])['project'] == self.JENKINS_PROJECT
    assert json.loads(packer_add_args[ARG_METADATA])['build_number'] == self.REAL_BUILD_NUMBER

  def should_upload_correct_package_contents(self):
    packer_add_args = self.mock_packer.calls('add')[DINGUS_FIRST_CALL][DINGUS_POS_ARGS]
    if self.NUM_ARTIFACTS > 1:
      assert packer_add_args[ARG_FILE].endswith(self.JENKINS_PROJECT + '.zip')
      with open_zip(packer_add_args[ARG_FILE], 'r') as artifacts_zip:
        for artifact in self.mock_artifacts:
          assert artifact.filename in artifacts_zip.namelist()
          assert artifacts_zip.read(artifact.filename, 'rb') == artifact.contents
    else:
      assert packer_add_args[ARG_FILE].endswith('/' + self.mock_artifacts[0].filename)

      with open(packer_add_args[ARG_FILE], 'rb') as fp:
        assert fp.read() == self.mock_artifacts[0].contents


class TestSingleUnpublishedArtifactForLatestBuildNumber(BaseTest):
  def setup(self):
    super(TestSingleUnpublishedArtifactForLatestBuildNumber, self).setup()
    self.setup_default_mocks(
        jenkins_build_number='latest',
        previously_published=False,
    )


class TestSinglePreviouslyPublishedArtifactForLatestBuildNumber(BaseTest):
  def setup(self):
    super(TestSinglePreviouslyPublishedArtifactForLatestBuildNumber, self).setup()
    self.setup_default_mocks(
        jenkins_build_number='latest',
        previously_published=True,
    )


class TestSingleUnpublishedArtifactForSomeBuildNumber(BaseTest):
  def setup(self):
    super(TestSingleUnpublishedArtifactForSomeBuildNumber, self).setup()
    self.setup_default_mocks(
        jenkins_build_number='90',
        previously_published=False,
    )


class TestSinglePreviouslyPublishedArtifactForSomeBuildNumber(BaseTest):
  def setup(self):
    super(TestSinglePreviouslyPublishedArtifactForSomeBuildNumber, self).setup()
    self.setup_default_mocks(
        jenkins_build_number='90',
        previously_published=True,
    )


class TestMultipleUnpublishedArtifactsForLatestBuildNumber(BaseTest):
  def setup(self):
    super(TestMultipleUnpublishedArtifactsForLatestBuildNumber, self).setup()
    self.setup_default_mocks(
        jenkins_build_number='latest',
        previously_published=False,
        num_artifacts=3
    )


class TestMultiplePreviouslyPublishedArtifactsForLatestBuildNumber(BaseTest):
  def setup(self):
    super(TestMultiplePreviouslyPublishedArtifactsForLatestBuildNumber, self).setup()
    self.setup_default_mocks(
        jenkins_build_number='latest',
        previously_published=True,
        num_artifacts=10
    )


class TestMultipleUnpublishedArtifactsForSomeBuildNumber(BaseTest):
  def setup(self):
    super(TestMultipleUnpublishedArtifactsForSomeBuildNumber, self).setup()
    self.setup_default_mocks(
        jenkins_build_number='90',
        previously_published=False,
        num_artifacts=2
    )


class TestMultiplePreviouslyPublishedArtifactsForSomeBuildNumber(BaseTest):
  def setup(self):
    super(TestMultiplePreviouslyPublishedArtifactsForSomeBuildNumber, self).setup()
    self.setup_default_mocks(
        jenkins_build_number='90',
        previously_published=True,
        num_artifacts=5
    )
