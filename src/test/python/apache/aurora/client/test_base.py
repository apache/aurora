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
import unittest

from apache.aurora.client import base

from gen.apache.aurora.api.ttypes import (
    PopulateJobResult,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    TaskConfig
)


class TestBase(unittest.TestCase):

  def test_format_response_with_message(self):
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail(message='Error')])
    formatted = base.format_response(resp)
    assert formatted == 'Response from scheduler: ERROR (message: Error)'

  def test_format_response_with_details(self):
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail(message='Error')])
    formatted = base.format_response(resp)
    assert formatted == 'Response from scheduler: ERROR (message: Error)'

  def test_combine_messages(self):
    resp = Response(responseCode=ResponseCode.ERROR)
    assert base.combine_messages(resp) == ''
    resp = Response(responseCode=ResponseCode.ERROR, details=[])
    assert base.combine_messages(resp) == ''
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail(message='Error')])
    assert base.combine_messages(resp) == 'Error'
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail()])
    assert base.combine_messages(resp) == 'Unknown error'
    resp = Response(
        responseCode=ResponseCode.ERROR,
        details=[ResponseDetail(message='Error1'), ResponseDetail(message='Error2')])
    assert base.combine_messages(resp) == 'Error1, Error2'

  def test_get_populated_task_config_set(self):
    config = TaskConfig()
    resp = Response(responseCode=ResponseCode.OK, result=Result(populateJobResult=PopulateJobResult(
        taskConfig=config)))
    assert config == base.get_populated_task_config(resp)

  def test_get_populated_task_config_deprecated_set(self):
    config = TaskConfig()
    resp = Response(responseCode=ResponseCode.OK, result=Result(populateJobResult=PopulateJobResult(
        populatedDEPRECATED=set([config]))))
    assert config == base.get_populated_task_config(resp)
