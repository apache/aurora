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

from gen.apache.aurora.api.ttypes import Response, ResponseCode, ResponseDetail


class TestBase(unittest.TestCase):

  def test_format_response_with_message(self):
    resp = Response(responseCode=ResponseCode.ERROR, messageDEPRECATED='Error')
    formatted = base.format_response(resp)
    assert formatted == 'Response from scheduler: ERROR (message: Error)'

  def test_format_response_with_details(self):
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail(message='Error')])
    formatted = base.format_response(resp)
    assert formatted == 'Response from scheduler: ERROR (message: Error)'
