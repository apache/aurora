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

import requests
from pystachio import Default, String
from twitter.common import log

from apache.aurora.common.cluster import Cluster

ACCEPT_HEADER = {'Accept': 'application/vnd.docker.distribution.manifest.v2+json'}
MANIFESTS_URL = '%s/v2/%s/manifests/%s'
DEFAULT_DOCKER_REGISTRY_HOST = 'https://registry-1.docker.io'


class DockerRegistryTrait(Cluster.Trait):
  docker_registry = Default(String, DEFAULT_DOCKER_REGISTRY_HOST)   # noqa


class DockerClientException(Exception):
  pass


class DockerRegistryClient(object):
  @classmethod
  def _get_auth_token(cls, www_authenticate, headers=None):
    # solve the auth challenge
    args = {}
    for t in www_authenticate.split(' ')[1].split(','):
      k, v = t.replace('"', '').split('=')
      if k == 'realm':
        domain = v
      elif k != 'error':
        args[k] = v

    if not domain:
      raise DockerClientException('Unable to determine auth endpoint. %s' % headers)

    response = requests.get(domain, params=args, headers=headers)
    response.raise_for_status()
    if response.status_code == requests.codes.ok and 'token' in response.json():
      return response.json()['token']
    else:
      raise DockerClientException('Failed to get auth token. %s' % response.text)

  @classmethod
  def _get_auth_challenge(cls, response):
    if 'Www-Authenticate' not in response.headers:
      raise DockerClientException('No auth challenge. Www-Authenticate header not returned. %s'
        % response.headers)

    return response.headers.get('Www-Authenticate')

  @classmethod
  def _solve_auth_challenge(cls, response, registry):
    www_authenticate = cls._get_auth_challenge(response)
    challenge_type = www_authenticate.split(' ')[0]
    if 'Bearer' == challenge_type:
      return {'Authorization': 'Bearer %s' % cls._get_auth_token(www_authenticate)}
    elif 'Basic' == challenge_type:
      raise DockerClientException('Basic authorization not supported: %s' % www_authenticate)
    else:
      raise DockerClientException('Unknown auth challenge: %s' % www_authenticate)

  @classmethod
  def _resolve_image(cls, registry, name, tag, headers=None):
    url = MANIFESTS_URL % (registry, name, tag)
    response = requests.head(url, headers=headers)

    if response.status_code == requests.codes.unauthorized:
      # solve the auth challenge and retry again
      authorization = cls._solve_auth_challenge(response, registry)
      if headers is None:
        headers = dict()
      headers.update(authorization)
      response = requests.head(url, headers=headers)

      if response.status_code == requests.codes.unauthorized:
        # its a private repo, raise exception
        raise DockerClientException('Private Docker repository - %s:%s' % (name, tag))

    if response.status_code == requests.codes.ok:
      image_ref = '%s@%s' % (name, response.headers.get('Docker-Content-Digest'))

      if registry != DEFAULT_DOCKER_REGISTRY_HOST:
        image_ref = '%s/%s' % (urlparse(registry).netloc, image_ref)

      log.info('Resolved %s:%s => %s' % (name, tag, image_ref))
      return image_ref

    # something is wrong
    response.raise_for_status()
    raise DockerClientException('Unable to resolve image %s:%s' % (name, tag))

  @classmethod
  def _version_check(cls, registry):
    response = requests.head('%s/v2/' % registry)

    if response.status_code == requests.codes.unauthorized:
      # retry request with token received by solving auth challenge
      response = requests.head('%s/v2/' % registry,
        headers=cls._solve_auth_challenge(response, registry))

    if response.status_code == requests.codes.ok:
      return True

    # something is wrong
    response.raise_for_status()
    return False

  @classmethod
  def resolve(cls, cluster, name, tag='latest'):
    """
      Docker image reference:
        [REGISTRY_HOST[:REGISTRY_PORT]/]REPOSITORY[:TAG|@DIGEST]
      See https://github.com/docker/distribution/blob/master/reference/reference.go
    """

    # Image digests is only supported in registry v2 - https://github.com/docker/docker/pull/11109
    # Check the registry and confirm that it is version 2.
    cluster = cluster.with_trait(DockerRegistryTrait)
    if cls._version_check(cluster.docker_registry):
      return cls._resolve_image(cluster.docker_registry, name, tag, headers=ACCEPT_HEADER)
    else:
      raise DockerClientException('%s is not a registry v2' % cluster.docker_registry)
