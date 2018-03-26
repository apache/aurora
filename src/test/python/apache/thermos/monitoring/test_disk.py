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

import json
import os
from tempfile import mkstemp
from time import sleep
from unittest import TestCase

import httpretty
from requests import ConnectionError
from twitter.common import dirutil
from twitter.common.dirutil import safe_mkdtemp
from twitter.common.quantity import Amount, Data, Time

from apache.thermos.monitoring.disk import (
    DiskCollectorSettings,
    DuDiskCollector,
    MesosDiskCollector
)

TEST_AMOUNT_1 = Amount(100, Data.MB)
TEST_AMOUNT_2 = Amount(10, Data.MB)
TEST_AMOUNT_SUM = TEST_AMOUNT_1 + TEST_AMOUNT_2

LOOK_UP_ERROR_VALUE = -1


def make_file(size, dir):
  _, filename = mkstemp(dir=dir)
  with open(filename, 'w') as f:
    f.write('0' * int(size.as_(Data.BYTES)))

    # Workaround for AURORA-1956.  On macOS 10.13 with APFS, st_blocks is not
    # consistent with st_size.
    while dirutil.safe_size(filename) < int(size.as_(Data.BYTES)):
      f.write('0' * 1024)
  return filename


def _run_collector_tests(collector, target, wait):
  assert collector.value == 0

  collector.sample()
  wait()
  assert collector.value == 0

  f1 = make_file(TEST_AMOUNT_1, dir=target)
  wait()
  assert collector.value >= TEST_AMOUNT_1.as_(Data.BYTES)

  make_file(TEST_AMOUNT_2, dir=target)
  wait()
  assert collector.value >= TEST_AMOUNT_SUM.as_(Data.BYTES)

  os.unlink(f1)
  wait()
  assert TEST_AMOUNT_SUM.as_(Data.BYTES) > collector.value >= TEST_AMOUNT_2.as_(Data.BYTES)


class TestDuDiskCollector(TestCase):
  def test_du_disk_collector(self):
    target = safe_mkdtemp()
    collector = DuDiskCollector(target)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    _run_collector_tests(collector, target, wait)


class TestMesosDiskCollector(TestCase):

  def setUp(self):
    self.agent_api_url = "http://localhost:5051/containers"
    self.executor_id_json_path = "executor_id"
    self.disk_usage_json_path = "statistics.disk_used_bytes"
    self.collection_timeout = Amount(1, Time.SECONDS)
    self.collection_interval = Amount(1, Time.SECONDS)

    self.sandbox_path = "/var/lib/path/to/thermos-some-task-id"

  @httpretty.activate
  def test_mesos_disk_collector(self):
    settings = DiskCollectorSettings(
      http_api_url=self.agent_api_url,
      executor_id_json_path=self.executor_id_json_path,
      disk_usage_json_path=self.disk_usage_json_path,
      disk_collection_timeout=self.collection_timeout,
      disk_collection_interval=self.collection_interval)

    first_json_body = json.dumps(
      [
        {
          "executor_id": "thermos-some-task-id",
          "statistics": {
            "disk_used_bytes": 100
          }
        }
      ]
    )
    second_json_body = json.dumps(
      [
        {
          "executor_id": "thermos-some-task-id",
          "statistics": {
            "disk_used_bytes": 200
          }
        }
      ]
    )
    httpretty.register_uri(
      method=httpretty.GET,
      uri="http://localhost:5051/containers",
      responses=[
        httpretty.Response(body=first_json_body, content_type='application/json'),
        httpretty.Response(body=second_json_body, content_type='application/json')])

    collector = MesosDiskCollector(self.sandbox_path, settings)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    assert collector.value == 0

    wait()
    assert collector.value == 100

    wait()
    assert collector.value == 200

    print (dir(httpretty.last_request()))
    self.assertEquals(httpretty.last_request().method, "GET")
    self.assertEquals(httpretty.last_request().path, "/containers")

  @httpretty.activate
  def test_mesos_disk_collector_bad_api_path(self):
    settings = DiskCollectorSettings(
      http_api_url="http://localhost:5051/wrong_path",
      executor_id_json_path=self.executor_id_json_path,
      disk_usage_json_path=self.disk_usage_json_path,
      disk_collection_timeout=self.collection_timeout,
      disk_collection_interval=self.collection_interval)

    json_body = json.dumps({"status": "bad_request"})
    httpretty.register_uri(
      method=httpretty.GET,
      uri="http://localhost:5051/containers",
      body=json_body,
      content_type='application/json')

    collector = MesosDiskCollector(self.sandbox_path, settings)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    assert collector.value == 0

    wait()
    assert collector.value == LOOK_UP_ERROR_VALUE

    self.assertEquals(httpretty.last_request().method, "GET")
    self.assertEquals(httpretty.last_request().path, "/wrong_path")

  @httpretty.activate
  def test_mesos_disk_collector_unexpected_response_format(self):
    settings = DiskCollectorSettings(
      http_api_url=self.agent_api_url,
      executor_id_json_path=self.executor_id_json_path,
      disk_usage_json_path=self.disk_usage_json_path,
      disk_collection_timeout=self.collection_timeout,
      disk_collection_interval=self.collection_interval)

    json_body = json.dumps({"status": "bad_request"})
    httpretty.register_uri(
      method=httpretty.GET,
      uri="http://localhost:5051/containers",
      body=json_body,
      content_type='application/json')

    collector = MesosDiskCollector(self.sandbox_path, settings)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    assert collector.value == 0

    wait()
    assert collector.value == LOOK_UP_ERROR_VALUE

    self.assertEquals(httpretty.last_request().method, "GET")
    self.assertEquals(httpretty.last_request().path, "/containers")

  @httpretty.activate
  def test_mesos_disk_collector_bad_executor_id_selector(self):
    settings = DiskCollectorSettings(
      http_api_url=self.agent_api_url,
      executor_id_json_path="bad_path",
      disk_usage_json_path=self.disk_usage_json_path,
      disk_collection_timeout=self.collection_timeout,
      disk_collection_interval=self.collection_interval)

    json_body = json.dumps(
      [
        {
          "executor_id": "thermos-some-task-id",
          "statistics": {
            "disk_used_bytes": 100
          }
        }
      ]
    )
    httpretty.register_uri(
      method=httpretty.GET,
      uri="http://localhost:5051/containers",
      body=json_body,
      content_type='application/json')

    collector = MesosDiskCollector(self.sandbox_path, settings)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    self.assertEquals(collector.value, 0)

    wait()
    self.assertEquals(collector.value, LOOK_UP_ERROR_VALUE)

    self.assertEquals(httpretty.last_request().method, "GET")
    self.assertEquals(httpretty.last_request().path, "/containers")

  @httpretty.activate
  def test_mesos_disk_collector_bad_disk_usage_selector(self):
    settings = DiskCollectorSettings(
      http_api_url=self.agent_api_url,
      executor_id_json_path=self.executor_id_json_path,
      disk_usage_json_path="bad_path",
      disk_collection_timeout=self.collection_timeout,
      disk_collection_interval=self.collection_interval)

    json_body = json.dumps(
      [
        {
          "executor_id": "thermos-some-task-id",
          "statistics": {
            "disk_used_bytes": 100
          }
        }
      ]
    )
    httpretty.register_uri(
      method=httpretty.GET,
      uri="http://localhost:5051/containers",
      responses=[httpretty.Response(body=json_body, content_type='application/json')])

    sandbox_path = "/var/lib/path/to/thermos-some-task-id"
    collector = MesosDiskCollector(sandbox_path, settings)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    self.assertEquals(collector.value, 0)

    wait()
    self.assertEquals(collector.value, LOOK_UP_ERROR_VALUE)

    self.assertEquals(httpretty.last_request().method, "GET")
    self.assertEquals(httpretty.last_request().path, "/containers")

  @httpretty.activate
  def test_mesos_disk_collector_when_unauthorized(self):
    settings = DiskCollectorSettings(
      http_api_url=self.agent_api_url,
      executor_id_json_path=self.executor_id_json_path,
      disk_usage_json_path=self.disk_usage_json_path,
      disk_collection_timeout=self.collection_timeout,
      disk_collection_interval=self.collection_interval)

    json_body = json.dumps(
      [
        {
          "executor_id": "thermos-some-task-id",
          "statistics": {
            "disk_used_bytes": 100
          }
        }
      ]
    )
    httpretty.register_uri(
      method=httpretty.GET,
      uri="http://localhost:5051/containers",
      body=json_body,
      status=401)

    collector = MesosDiskCollector(self.sandbox_path, settings)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    self.assertEquals(collector.value, 0)

    wait()
    self.assertEquals(collector.value, LOOK_UP_ERROR_VALUE)

    self.assertEquals(httpretty.last_request().method, "GET")
    self.assertEquals(httpretty.last_request().path, "/containers")

  @httpretty.activate
  def test_mesos_disk_collector_when_connection_error(self):
    settings = DiskCollectorSettings(
      http_api_url=self.agent_api_url,
      executor_id_json_path=self.executor_id_json_path,
      disk_usage_json_path=self.disk_usage_json_path,
      disk_collection_timeout=self.collection_timeout,
      disk_collection_interval=self.collection_interval
    )

    def exception_callback(request, uri, headers):
      raise ConnectionError

    httpretty.register_uri(
      method=httpretty.GET,
      uri="http://localhost:5051/containers",
      body=exception_callback
    )

    collector = MesosDiskCollector(self.sandbox_path, settings)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    self.assertEquals(collector.value, 0)

    wait()
    self.assertEquals(collector.value, LOOK_UP_ERROR_VALUE)

    self.assertEquals(httpretty.last_request().method, "GET")
    self.assertEquals(httpretty.last_request().path, "/containers")

  @httpretty.activate
  def test_mesos_disk_collector_timeout(self):
    settings = DiskCollectorSettings(
      http_api_url=self.agent_api_url,
      executor_id_json_path=self.executor_id_json_path,
      disk_usage_json_path=self.disk_usage_json_path,
      disk_collection_timeout=Amount(10, Time.MILLISECONDS),
      disk_collection_interval=self.collection_interval
    )

    def callback(request, uri, headers):
      json_body = json.dumps(
        [
          {
            "executor_id": "thermos-some-task-id",
            "statistics": {
              "disk_used_bytes": 100
            }
          }
        ]
      )
      sleep(5)
      return (200, headers, json_body)

    httpretty.register_uri(
      method=httpretty.GET,
      uri="http://localhost:5051/containers",
      body=callback
    )

    collector = MesosDiskCollector(self.sandbox_path, settings)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    self.assertEquals(collector.value, 0)

    wait()
    self.assertEquals(collector.value, LOOK_UP_ERROR_VALUE)

    self.assertEquals(httpretty.last_request().method, "GET")
    self.assertEquals(httpretty.last_request().path, "/containers")
