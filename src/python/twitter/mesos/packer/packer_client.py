from __future__ import print_function

import getpass
import hashlib
import httplib
import json
import os
import socket
import sys
import time
import urllib
import urllib2

from poster.encode import multipart_encode
from poster.streaminghttp import StreamingHTTPHandler

from twitter.common import log
from twitter.common_internal.auth.ssh import SSHAgentAuthenticator


class Progress(object):
  def __init__(self):
    self._seen = 0

  def emit(self, value):
    sys.stdout.write(value + '\r')
    sys.stdout.flush()

  def update(self, total, size, name):
    self._seen += size
    pct = self._seen * 100 / total
    self.emit('%s: %s%%' % (name, pct))


class QuietProgress(Progress):
  def emit(self, value):
    pass


class CallbackFile(file):
  def __init__(self, path, mode, callback, *args):
    super(CallbackFile, self).__init__(path, mode)
    self._total = os.path.getsize(path)
    self._callback = callback
    self._args = args

  def __len__(self):
    return self._total

  def read(self, size):
    data = super(CallbackFile, self).read(size)
    self._callback(self._total, len(data), *self._args)
    return data


class Packer(object):
  class Error(Exception): pass

  def __init__(self, host, port, verbose=True):
    self._host = host
    self._port = port
    self._client = None
    self._verbose = verbose

  def _get_client(self):
    # TODO(William Farner): Use HTTPS (MESOS-873)
    if self._client is None:
      self._client = httplib.HTTPConnection(self._host, self._port)
    return self._client

  @staticmethod
  def compose_url(endpoint, query_params={}, auth=False):
    url = endpoint
    if auth:
      user = getpass.getuser()
      nonce, nonce_sig = SSHAgentAuthenticator.create_session(user)
      auth_params = { 'user': user, 'nonce': nonce, 'token': nonce_sig.encode('hex') }
      query_params = dict(query_params.items() + auth_params.items())
    if query_params:
      url += '?%s' % urllib.urlencode(query_params)
    return url

  def _api(self, endpoint, params={}, body=None, headers={}, method='GET', auth=False):
    conn = self._get_client()
    conn.request(method, Packer.compose_url(endpoint, params, auth), body=body, headers=headers)
    resp = conn.getresponse()
    resp_type = resp.status / 100
    if resp_type not in [2, 3]:
      raise Packer.Error(resp.read())
    return resp.read()

  @staticmethod
  def _role_url(role):
    return '/package/%s' % role

  @staticmethod
  def _pkg_url(role, package):
    return '%s/%s' % (Packer._role_url(role), package)

  @staticmethod
  def _live_url(role, package):
    return '%s/%s/live' % (Packer._role_url(role), package)

  @staticmethod
  def _ver_url(role, package, version):
    # Quoting the path to support arbitrary version identifiers such as metadata strings.
    return '%s/%s' % (Packer._pkg_url(role, package), urllib.quote(version))

  def compute_checksum(self, local_file):
    # Calculate the checksum, reading 16 MiB chunks.
    checksum_progress = Progress() if self._verbose else QuietProgress()
    stream = CallbackFile(local_file, 'rb', checksum_progress.update, 'Calculating checksum')
    md5 = hashlib.md5()
    while True:
      data = stream.read(16777216)
      if not data:
        break
      md5.update(data)
    return md5.hexdigest()

  def list_packages(self, role):
    return json.loads(self._api(Packer._role_url(role)))

  def get_version(self, role, package, version):
    data = json.loads(self._api(Packer._ver_url(role, package, version)))
    log.debug('Metadata: %s' % data)
    return data

  def list_versions(self, role, package):
    return json.loads(self._api(Packer._pkg_url(role, package)))

  def delete(self, role, package, version):
    return self._api(Packer._ver_url(role, package, version), auth=True, method='DELETE')

  def add(self, role, package, local_file, metadata, digest=None):
    if digest is None:
      digest = self.compute_checksum(local_file)
    return self._add(role, package, local_file, metadata, digest)

  def _add(self, role, package, local_file, metadata, digest):
    if self._verbose:
      print()
      upload_progress = Progress()
    else:
      upload_progress = QuietProgress()
    stream = CallbackFile(local_file, 'rb', upload_progress.update, 'Uploading')
    datagen, headers = multipart_encode({'file': stream})
    selector = Packer.compose_url('/data/%s' % digest, auth=True)
    url = 'http://%s:%s%s' % (self._host, self._port, selector)
    log.debug('Uploading package data blob: %s' % url)

    file_size = os.path.getsize(local_file)
    upload_start = time.time()
    try:
      request = urllib2.Request(url, datagen, headers)
      opener = urllib2.build_opener(StreamingHTTPHandler)
      conn = opener.open(request, None, socket._GLOBAL_DEFAULT_TIMEOUT)
      status = conn.getcode()

      if status == 201:
        upload_secs = time.time() - upload_start
        log.debug('Average upload rate: %s KB/s' % (int(file_size / 1024 / upload_secs)))
      elif status == 200:
        log.debug('Packer already has this data blob, not uploading again.')
      else:
        raise Packer.Error('Package data not uploaded: %s' % conn.read())

      # We can now safely assume the data blob is present, add the version.
      query_params = {'filename': os.path.basename(local_file), 'md5sum': digest}
      if metadata:
        query_params['metadata'] = metadata
      return json.loads(self._api(Packer._pkg_url(role, package),
                                  auth=True,
                                  params=query_params,
                                  method='POST'))
    except urllib2.HTTPError as e:
      raise Packer.Error('HTTP %s: %s' % (e.code, e.msg))
    except urllib2.URLError as e:
      raise Packer.Error('Failed to upload to packer: %s' % e)

  def unlock(self, role, package):
    return self._api('%s/unlock' % Packer._pkg_url(role, package), auth=True, method='POST')

  def set_live(self, role, package, version):
    return self._api(Packer._live_url(role, package),
                     auth=True,
                     body='version=%s' % version,
                     method='PUT')

  def unset_live(self, role, package):
    return self._api(Packer._live_url(role, package), auth=True, method='DELETE')
