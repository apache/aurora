from __future__ import print_function

import getpass
import hashlib
import httplib
import json
import mimetypes
import os
import sys
import time
import urllib
import urllib2

from poster.encode import multipart_encode
from poster import streaminghttp

from twitter.mesos.client_wrapper import MesosHelper


class Progress(object):

  def __init__(self):
    self._seen = 0

  def update(self, total, size, name):
    self._seen += size
    pct = self._seen * 100 / total
    sys.stdout.write('%s: %s%%\r' % (name, pct))
    sys.stdout.flush()


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

  def __init__(self, host, port):
    self._host = host
    self._port = port
    self._client = None

  def _get_client(self):
    # TODO(William Farner): Use HTTPS (MESOS-873)
    if self._client is None:
      self._client = httplib.HTTPConnection(self._host, self._port)
    return self._client

  @staticmethod
  def compose_url(endpoint, query_params={}, auth=False):
    url = endpoint
    if auth:
      session_key = MesosHelper.acquire_session_key(getpass.getuser())
      auth_params = {
        'user': getpass.getuser(),
        'nonce': session_key.nonce,
        'token': session_key.nonceSig.encode('hex')
      }
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
  def _ver_url(role, package, version):
    return '%s/%s' % (Packer._pkg_url(role, package), version)

  def list_packages(self, role):
    return json.loads(self._api(Packer._role_url(role)))

  def list_versions(self, role, package):
    return json.loads(self._api(Packer._pkg_url(role, package)))

  def delete(self, role, package, version):
    return self._api(Packer._ver_url(role, package, version), auth=True, method='DELETE')

  def add(self, role, package, local_file):
    streaminghttp.register_openers()

    # Calculate the checksum, reading 16 MiB chunks.
    checksum_progress = Progress()
    stream = CallbackFile(local_file, 'rb', checksum_progress.update, 'Calculating checksum')
    md5 = hashlib.md5()
    while True:
      data = stream.read(16777216)
      if not data:
        break
      md5.update(data)
    digest = md5.hexdigest()

    # TODO(wfarner): Come up with a way to show upload progress.
    print()
    upload_progress = Progress()
    stream = CallbackFile(local_file, 'rb', upload_progress.update, 'Uploading')
    datagen, headers = multipart_encode({'file': stream})

    selector = Packer.compose_url(Packer._pkg_url(role, package), {'md5sum': digest}, auth=True)
    url = 'http://%s:%s%s' % (self._host, self._port, selector)

    file_size = os.path.getsize(local_file)
    upload_start = time.time()
    try:
      request = urllib2.Request(url, datagen, headers)
      resp = urllib2.urlopen(request).read()
      upload_secs = time.time() - upload_start
      print('Average upload rate: %s KB/s' % (int(file_size / 1024 / upload_secs)))
      return json.loads(resp)
    except urllib2.HTTPError as e:
      raise Packer.Error(e.read())

  def unlock(self, role, package):
    return self._api('%s/unlock' % Packer._pkg_url(role, package), auth=True, method='POST')

  def set_live(self, role, package, version):
    return self._api('%s/live' % Packer._ver_url(role, package, version), auth=True, method='POST')
