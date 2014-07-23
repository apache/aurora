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
import time
import unittest

from twitter.common.quantity import Amount, Time

from apache.aurora.client.api.scheduler_mux import SchedulerMux


class SchedulerMuxTest(unittest.TestCase):

  DATA = [1, 2, 3]
  MUX = None

  @classmethod
  def setUpClass(cls):
    cls.MUX = SchedulerMux(wait_timeout=Amount(10, Time.MILLISECONDS))

  @classmethod
  def tearDownClass(cls):
    cls.MUX.terminate()

  @classmethod
  def error_command(cls, data):
    raise SchedulerMux.Error('expected')

  @classmethod
  def unknown_error_command(cls, data):
    raise Exception('expected')

  @classmethod
  def timeout_command(cls, data):
    time.sleep(2)

  def test_success(self):
    assert [self.DATA] == self.MUX.enqueue_and_wait(lambda d: d, self.DATA)

  def test_failure(self):
    try:
      self.MUX.enqueue_and_wait(self.error_command, self.DATA)
    except SchedulerMux.Error as e:
      assert 'expected' in e.message
    else:
      self.fail()

  def test_unknown_failure(self):
    try:
      self.MUX.enqueue_and_wait(self.unknown_error_command, self.DATA)
    except SchedulerMux.Error as e:
      assert 'Unknown error' in e.message
    else:
      self.fail()

  def test_timeout(self):
    try:
      self.MUX.enqueue_and_wait(self.timeout_command, self.DATA, timeout=Amount(1, Time.SECONDS))
    except SchedulerMux.Error as e:
      'Failed to complete operation' in e.message
    else:
      self.fail()
