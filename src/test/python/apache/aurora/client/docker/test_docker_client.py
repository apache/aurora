#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from urlparse import urlparse

import pytest
import requests
import requests_mock

from apache.aurora.client.docker.docker_client import (
    ACCEPT_HEADER,
    DEFAULT_DOCKER_REGISTRY_HOST,
    MANIFESTS_URL,
    DockerClientException,
    DockerRegistryClient,
    DockerRegistryTrait
)
from apache.aurora.common.clusters import Cluster

REGISTRY_URL = 'https://registry.example.com'
NAME = 'library/ubuntu'
TAG = 'latest'
IMAGE_DIGEST = 'sha256:45bc58500fa3d3c0d67233d4a7798134b46b486af1389ca87000c543f46c3d24'
EXPECTED_REFERENCE = '%s/%s@%s' % (urlparse(REGISTRY_URL).netloc, NAME, IMAGE_DIGEST)


def test_docker_registry_trait_default():
  cluster = Cluster().with_trait(DockerRegistryTrait)
  assert cluster.docker_registry == DEFAULT_DOCKER_REGISTRY_HOST


def test_docker_binding_version_check_failure():
  do_test_docker_binding_failure(version_success=False)


def test_docker_binding_tag_not_found():
  do_test_docker_binding_failure(manifests_success=False)


def test_docker_binding_auth_token_failure():
  do_test_docker_binding_failure(auth_success=False)


def do_test_docker_binding_failure(version_success=True, manifests_success=True,
  auth_success=True):
  with requests_mock.Mocker() as m:
    mock_registry(m, need_auth_token=True, version_success=version_success,
      manifests_success=manifests_success, auth_success=auth_success)

    with pytest.raises(DockerClientException):
      DockerRegistryClient.resolve(mock_cluster(), NAME, TAG)


def test_docker_binding_unknown_registry():
  with pytest.raises(Exception):
    DockerRegistryClient.resolve(mock_cluster(), NAME, TAG)


def test_docker_binding_public_repo():
  do_test_docker_binding_success()


def test_docker_binding_with_auth_public_repo():
  do_test_docker_binding_success(need_auth_token=True)


def do_test_docker_binding_success(need_auth_token=False):
  with requests_mock.Mocker() as m:
    mock_registry(m, need_auth_token=need_auth_token)
    assert DockerRegistryClient.resolve(mock_cluster(), NAME, TAG) == EXPECTED_REFERENCE


def mock_version(request, context):
  context.status_code = requests.codes.ok
  return {}


def mock_version_not_supported(request, context):
  context.status_code = requests.codes.moved_permanently
  return {'error': 'version not found'}


def mock_version_with_auth(request, context):
  if request.headers.get('Authorization') == 'Bearer some-token':
    return mock_version(request, context)
  else:
    context.status_code = requests.codes.unauthorized
    context.headers.update(
      {'Www-Authenticate': 'Bearer realm="https://auth.docker.io/token",'
      + 'service="registry.docker.io"'})
    return {'error': 'unauthorized'}


def mock_manifests(request, context):
  if request.headers['Accept'] == ACCEPT_HEADER['Accept']:
    context.status_code = requests.codes.ok
    context.headers.update({'Docker-Content-Digest': IMAGE_DIGEST})
    return {}
  else:
    raise Exception("Missing Accept header")


def mock_manifests_tag_failure(request, context):
  context.status_code = requests.codes.moved_permanently
  return {'error': 'tag not found'}


def mock_manifests_with_auth(request, context):
  if request.headers.get('Authorization') == 'Bearer some-token':
    return mock_manifests(request, context)
  else:
    context.status_code = requests.codes.unauthorized
    context.headers.update(
      {'Www-Authenticate': 'Bearer realm="https://auth.docker.io/token",'
      + 'service="registry.docker.io",scope="repository:library/ubuntu:*"'})
    return {'error': 'unauthorized'}


def mock_token(request, context):
  return {'token': 'some-token'}


def mock_token_failure(request, context):
  context.status_code = requests.codes.moved_permanently
  return {'error': 'token not found'}


def mock_token_with_creds(request, context):
  if request.headers.get('Authorization') == 'Basic some-auth':
    return mock_token(request, context)
  else:
    return {'token': 'wrong-token'}


def mock_registry(m, need_auth_token=False, auth_success=True,
  version_success=True, manifests_success=True):
  if need_auth_token:
    m.register_uri('HEAD', '%s/v2/' % REGISTRY_URL,
      [{'json': mock_version_with_auth if version_success else mock_version_not_supported}])
    m.register_uri('GET', 'https://auth.docker.io/token?service=registry.docker.io',
      [{'json': mock_token if auth_success else mock_token_failure}])
    m.register_uri('HEAD', MANIFESTS_URL % (REGISTRY_URL, NAME, TAG),
        [{'json': mock_manifests_with_auth if manifests_success else mock_manifests_tag_failure}])
  else:
    m.register_uri('HEAD', '%s/v2/' % REGISTRY_URL,
      [{'json': mock_version if version_success else mock_version_not_supported}])
    m.register_uri('HEAD', MANIFESTS_URL % (REGISTRY_URL, NAME, TAG),
        [{'json': mock_manifests if manifests_success else mock_manifests_tag_failure}])


def mock_cluster(docker_registry=REGISTRY_URL):
  return Cluster(name='smf1-test', docker_registry=docker_registry)
