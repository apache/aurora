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

"""Represent resource consumption statistics for processes

This module exposes one class: the ProcessSample, used to represent resource consumption. A single
ProcessSample might correspond to one individual process, or to an aggregate of multiple processes.

"""

from collections import namedtuple


class ProcessSample(namedtuple('ProcessSample', 'rate user system rss vms nice status threads')):
  """ Sample of statistics about a process's resource consumption (either a single process or an
  aggregate of processes) """

  @staticmethod
  def empty():
    return ProcessSample(rate=0, user=0, system=0, rss=0, vms=0, nice=None, status=None, threads=0)

  def __add__(self, other):
    if self.nice is not None and other.nice is None:
      nice = self.nice
    else:
      nice = other.nice
    if self.status is not None and other.status is None:
      status = self.status
    else:
      status = other.status
    return ProcessSample(
      rate = self.rate + other.rate,
      user = self.user + other.user,
      system = self.system + other.system,
      rss = self.rss + other.rss,
      vms = self.vms + other.vms,
      nice = nice,
      status = status,
      threads = self.threads + other.threads)

  def to_dict(self):
    return dict(
      cpu     = self.rate,
      ram     = self.rss,
      user    = self.user,
      system  = self.system,
      rss     = self.rss,
      vms     = self.vms,
      nice    = self.nice,
      status  = str(self.status),
      threads = self.threads
    )
