#
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

from abc import abstractmethod

from twitter.common.lang import Interface

from .status_checker import StatusChecker


class TaskError(Exception):
  pass


class TaskRunner(StatusChecker):
  # For now, TaskRunner should just maintain the StatusChecker API.
  pass


class TaskRunnerProvider(Interface):
  @abstractmethod
  def from_assigned_task(self, assigned_task, sandbox):
    pass
