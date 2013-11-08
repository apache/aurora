from twitter.aurora.client.hooks.hooked_api import HookedAuroraClientAPI

from gen.twitter.aurora.ttypes import (
    Response,
    ResponseCode,
    Result,
)

from mock import Mock


def create_blank_response(code, msg):
  response = Mock(spec=Response)
  response.responseCode = code
  response.message = msg
  response.result = Mock(spec=Result)
  return response


def create_simple_success_response():
  return create_blank_response(ResponseCode.OK, 'OK')


def create_mock_api():
  """Builds up a mock API object, with a mock SchedulerProxy"""
  mock_api = Mock(spec=HookedAuroraClientAPI)
  mock_scheduler = Mock()
  mock_scheduler.url = "http://something_or_other"
  mock_scheduler_client = Mock()
  mock_scheduler_client.scheduler.return_value = mock_scheduler
  mock_scheduler_client.url = "http://something_or_other"
  mock_api = Mock(spec=HookedAuroraClientAPI)
  mock_api.scheduler = mock_scheduler_client
  return (mock_api, mock_scheduler_client)
