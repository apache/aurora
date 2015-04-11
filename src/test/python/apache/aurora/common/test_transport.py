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
from threading import Thread

import pytest
import requests
from mock import ANY, call, create_autospec, Mock
from requests import exceptions as request_exceptions
from thrift.protocol import TJSONProtocol
from thrift.server import THttpServer
from thrift.transport import TTransport

from apache.aurora.common.transport import DEFAULT_USER_AGENT, TRequestsTransport

from gen.apache.aurora.api import ReadOnlyScheduler
from gen.apache.aurora.api.ttypes import Response, ResponseCode, ServerInfo


class ReadOnlySchedulerHandler(object):
  def getRoleSummary(self):  # noqa
    server_info = ServerInfo(clusterName='west', thriftAPIVersion=3)
    return Response(responseCode=ResponseCode.OK, serverInfo=server_info)


def test_request_transport_integration():
  handler = ReadOnlySchedulerHandler()
  processor = ReadOnlyScheduler.Processor(handler)
  pfactory = TJSONProtocol.TJSONProtocolFactory()
  server = THttpServer.THttpServer(processor, ('localhost', 0), pfactory)
  server_thread = Thread(target=server.serve)
  server_thread.start()
  _, server_port = server.httpd.socket.getsockname()

  try:
    transport = TRequestsTransport('http://localhost:%d' % server_port)
    protocol = TJSONProtocol.TJSONProtocol(transport)
    client = ReadOnlyScheduler.Client(protocol)
    response = client.getRoleSummary()
  finally:
    server.httpd.shutdown()

  assert response is not None
  assert response.responseCode == ResponseCode.OK
  assert response.serverInfo.clusterName == 'west'
  assert response.serverInfo.thriftAPIVersion == 3

  transport.close()


def test_request_transport_timeout():
  session = create_autospec(spec=requests.Session, instance=True)
  session.headers = {}
  session.post = Mock(side_effect=request_exceptions.Timeout())
  transport = TRequestsTransport('http://localhost:12345', session_factory=lambda: session)
  protocol = TJSONProtocol.TJSONProtocol(transport)
  client = ReadOnlyScheduler.Client(protocol)

  with pytest.raises(TTransport.TTransportException) as execinfo:
    client.getRoleSummary()

  assert execinfo.value.message == 'Timed out talking to http://localhost:12345'

  transport.close()


def test_raise_for_status_causes_exception():
  response = create_autospec(spec=requests.Response, instance=True)
  response.headers = {'header1': 'data', 'header2': 'data2'}
  response.raise_for_status.side_effect = requests.exceptions.HTTPError()

  session = create_autospec(spec=requests.Session, instance=True)
  session.headers = {}
  session.post.return_value = response

  transport = TRequestsTransport('http://localhost:12345', session_factory=lambda: session)
  protocol = TJSONProtocol.TJSONProtocol(transport)
  client = ReadOnlyScheduler.Client(protocol)

  with pytest.raises(TTransport.TTransportException) as excinfo:
    client.getRoleSummary()

  assert excinfo.value.type == TTransport.TTransportException.UNKNOWN
  assert excinfo.value.message.startswith('Unknown error talking to http://localhost:12345')

  transport.close()

  response.raise_for_status.assert_called_once_with()


def test_request_any_other_exception():
  session = create_autospec(spec=requests.Session, instance=True)
  session.headers = {}
  session.post = Mock(side_effect=request_exceptions.ConnectionError())
  transport = TRequestsTransport('http://localhost:12345', session_factory=lambda: session)
  protocol = TJSONProtocol.TJSONProtocol(transport)
  client = ReadOnlyScheduler.Client(protocol)

  with pytest.raises(TTransport.TTransportException):
    client.getRoleSummary()

  transport.close()


def test_requests_transports_lowers_logging_level():
  logging.getLogger('requests').setLevel(logging.NOTSET)

  TRequestsTransport(
      'http://localhost:12345',
      session_factory=lambda x: create_autospec(spec=requests.Session, instance=True))

  assert logging.getLogger('requests').level == logging.WARNING


def test_transport_applies_user_agent_from_factory():
  user_agent = 'Some-User-Agent'
  transport = TRequestsTransport('http://localhost:12345', user_agent=user_agent)
  transport.open()
  assert transport._session.headers['User-Agent'] == user_agent


def test_transport_applies_default_user_agent_if_no_factory_provided():
  transport = TRequestsTransport('http://localhost:12345')
  transport.open()
  assert transport._session.headers['User-Agent'] == DEFAULT_USER_AGENT


def test_auth_type_valid():
  response = create_autospec(spec=requests.Response, instance=True)
  response.headers = {'header1': 'data', 'header2': 'data2'}
  response.raise_for_status.side_effect = requests.exceptions.HTTPError()

  session = create_autospec(spec=requests.Session, instance=True)
  session.headers = {}
  session.post.return_value = response

  auth = requests.auth.AuthBase()
  transport = TRequestsTransport('http://localhost:1', auth=auth, session_factory=lambda: session)
  protocol = TJSONProtocol.TJSONProtocol(transport)
  client = ReadOnlyScheduler.Client(protocol)

  with pytest.raises(TTransport.TTransportException):
    client.getRoleSummary()

  transport.close()

  session.post.mock_calls = (call(ANY, data=ANY, timeout=ANY, auth=auth))


def test_auth_type_invalid():
  with pytest.raises(TypeError) as e:
    TRequestsTransport('http://localhost:1', auth="auth")
  assert e.value.message == 'Invalid auth type. Expected: AuthBase but got str'
