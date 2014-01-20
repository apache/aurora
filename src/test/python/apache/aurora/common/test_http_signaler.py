#
# Copyright 2013 Apache Software Foundation
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

from socket import timeout as SocketTimeout
import unittest

from twitter.common.lang import Compatibility

from apache.aurora.common.http_signaler import HttpSignaler

import mox

if Compatibility.PY3:
  import urllib.request as urllib_request
else:
  import urllib2 as urllib_request

StringIO = Compatibility.StringIO


class TestHttpSignaler(unittest.TestCase):
  PORT = 12345

  def setUp(self):
    self._mox = mox.Mox()

  def tearDown(self):
    self._mox.UnsetStubs()
    self._mox.VerifyAll()

  def test_all_calls_ok(self):
    self._mox.StubOutWithMock(urllib_request, 'urlopen')
    urllib_request.urlopen(
      'http://localhost:%s/health' % self.PORT, None, timeout=1.0).AndReturn(StringIO('ok'))
    urllib_request.urlopen(
      'http://localhost:%s/quitquitquit' % self.PORT, '', timeout=1.0).AndReturn(StringIO(''))
    urllib_request.urlopen(
      'http://localhost:%s/abortabortabort' % self.PORT, '', timeout=1.0).AndReturn(StringIO(''))

    self._mox.ReplayAll()

    signaler = HttpSignaler(self.PORT)
    assert signaler.health() == (True, None)
    assert signaler.quitquitquit() == (True, None)
    assert signaler.abortabortabort() == (True, None)

  def test_health_not_ok(self):
    self._mox.StubOutWithMock(urllib_request, 'urlopen')
    urllib_request.urlopen(
        'http://localhost:%s/health' % self.PORT, None, timeout=1.0).AndReturn(StringIO('not ok'))

    self._mox.ReplayAll()

    health, reason = HttpSignaler(self.PORT).health()
    assert not health
    assert reason.startswith('Response differs from expected response')

  def test_exception(self):
    self._mox.StubOutWithMock(urllib_request, 'urlopen')
    urllib_request.urlopen(
        'http://localhost:%s/health' % self.PORT, None, timeout=1.0).AndRaise(
        SocketTimeout('Timed out'))

    self._mox.ReplayAll()

    assert not HttpSignaler(self.PORT).health()[0]
