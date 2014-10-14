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

import logging
from io import BytesIO

import requests
from requests import exceptions as request_exceptions
from thrift.transport.TTransport import TTransportBase, TTransportException

try:
  from urlparse import urlparse
except ImportError:
  from urllib.parse import urlparse


def default_requests_session_factory():
  session = requests.session()
  session.headers['User-Agent'] = 'Python TRequestsTransport v1.0'
  return session


class TRequestsTransport(TTransportBase):
  """A Thrift HTTP client based upon the requests module."""

  def __init__(self, uri, auth=None, session_factory=default_requests_session_factory):
    """Construct a TRequestsTransport.

    Construct a Thrift transport based upon the requests module.  URI is the
    HTTP endpoint that the server should be listening on.  The 'auth'
    keyword is passed directly to the requests client and can be used to
    provide different authentication contexts such as Kerberos
    authentication via the requests-kerberos module.

    :param uri: The endpoint uri
    :type uri: str
    :keyword auth: The requests authentication context.
    """
    self.__session = None
    self.__session_factory = session_factory
    if not callable(session_factory):
      raise TypeError('session_factory should be a callable that produces a requests.Session!')
    self.__wbuf = BytesIO()
    self.__rbuf = BytesIO()
    self.__uri = uri
    try:
      self.__urlparse = urlparse(uri)
    except ValueError:
      raise TTransportException('Failed to parse uri %r' % (uri,))
    self.__timeout = None
    self.__auth = auth

    # Silence requests logs so we don't get messages for every HTTP connection.
    logging.getLogger('requests').setLevel(logging.WARNING)

  def isOpen(self):
    return self.__session is not None

  def open(self):
    self.__session = self.__session_factory()

  def close(self):
    session, self.__session = self.__session, None
    session.close()

  def setTimeout(self, ms):
    self.__timeout = ms / 1000.0

  def read(self, size):
    return self.__rbuf.read(size)

  def write(self, buf):
    self.__wbuf.write(buf)

  def flush(self):
    if self.isOpen():
      self.close()

    self.open()

    data = self.__wbuf.getvalue()
    self.__wbuf = BytesIO()

    self.__session.headers['Content-Type'] = 'application/x-thrift'
    self.__session.headers['Content-Length'] = str(len(data))
    self.__session.headers['Host'] = self.__urlparse.hostname

    try:
      response = self.__session.post(
          self.__uri,
          data=data,
          timeout=self.__timeout,
          auth=self.__auth)
    except request_exceptions.Timeout:
      raise TTransportException(
          type=TTransportException.TIMED_OUT,
          message='Timed out talking to %s' % self.__uri)
    except request_exceptions.RequestException as e:
      raise TTransportException(
          type=TTransportException.UNKNOWN,
          message='Unknown error talking to %s: %s' % (self.__uri, e))

    self.__rbuf = BytesIO(response.content)
