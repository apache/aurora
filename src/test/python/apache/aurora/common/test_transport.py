from threading import Thread

import mock
import requests
from requests import exceptions as request_exceptions
from thrift.protocol import TJSONProtocol
from thrift.server import THttpServer
from thrift.transport import TTransport

from apache.aurora.common.transport import TRequestsTransport

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

  response = None

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
  session = mock.MagicMock(spec=requests.Session)
  session.headers = {}
  session.post = mock.Mock(side_effect=request_exceptions.Timeout())
  transport = TRequestsTransport('http://localhost:12345', session_factory=lambda: session)
  protocol = TJSONProtocol.TJSONProtocol(transport)
  client = ReadOnlyScheduler.Client(protocol)

  try:
    client.getRoleSummary()
    assert False, 'getRoleSummary should not succeed'
  except TTransport.TTransportException as e:
    assert e.message == 'Timed out talking to http://localhost:12345'
  except Exception as e:
    assert False, 'Only expected TTransportException, got %s' % e

  transport.close()


def test_request_any_other_exception():
  session = mock.MagicMock(spec=requests.Session)
  session.headers = {}
  session.post = mock.Mock(side_effect=request_exceptions.ConnectionError())
  transport = TRequestsTransport('http://localhost:12345', session_factory=lambda: session)
  protocol = TJSONProtocol.TJSONProtocol(transport)
  client = ReadOnlyScheduler.Client(protocol)

  try:
    client.getRoleSummary()
    assert False, 'getRoleSummary should not succeed'
  except TTransport.TTransportException:
    pass
  except Exception as e:
    assert False, 'Only expected TTransportException, got %s' % e

  transport.close()
