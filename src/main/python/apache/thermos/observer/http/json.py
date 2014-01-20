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

import urllib

from twitter.common.http import HttpServer


class TaskObserverJSONBindings(object):
  """
    Mixin for Thermos observer JSON endpoints.
  """

  @HttpServer.route("/j/task_ids")
  @HttpServer.route("/j/task_ids/:which")
  @HttpServer.route("/j/task_ids/:which/:offset")
  @HttpServer.route("/j/task_ids/:which/:offset/:num")
  def handle_task_ids(self, which=None, offset=None, num=None):
    return self._observer.task_ids(
      which,
      int(offset) if offset is not None else 0,
      int(num) if num is not None else 20)

  @HttpServer.route("/j/task_id_count")
  def handle_task_id_count(self):
    return self._observer.task_id_count()

  @HttpServer.route("/j/task")
  def handle_tasks(self):
    """
      Additional parameters:
        task_id = comma separated list of task_ids.
    """
    task_ids = HttpServer.Request.GET.get('task_id', [])
    if task_ids:
      task_ids = urllib.unquote(task_ids).split(',')
    return self._observer.task(task_ids)

  @HttpServer.route("/j/task/:task_id")
  def handle_task(self, task_id):
    return self._observer.task([task_id])

  @HttpServer.route("/j/process/:task_id")
  @HttpServer.route("/j/process/:task_id/:process")
  @HttpServer.route("/j/process/:task_id/:process/:run")
  def handle_process(self, task_id, process=None, run=None):
    return self._observer.process(task_id, process, run)

  @HttpServer.route("/j/processes")
  def handle_processes(self):
    """
      Additional parameters:
        task_ids = comma separated list of task_ids.
    """
    task_ids = HttpServer.Request.GET.get('task_id', [])
    if task_ids:
      task_ids = urllib.unquote(task_ids).split(',')
    return self._observer.processes(task_ids)

