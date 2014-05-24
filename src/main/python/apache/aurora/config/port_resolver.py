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

from twitter.common.lang import Compatibility


class PortResolver(object):
  class CycleException(Exception): pass

  @classmethod
  def resolve(cls, portmap):
    """
        Given an announce-style portmap, return a fully dereferenced portmap.

        For example, given the portmap:
          {
            'http': 80,
            'aurora: 'http',
            'https': 'aurora',
            'thrift': 'service'
          }

        Returns {'http': 80, 'aurora': 80, 'https': 80, 'thrift': 'service'}
    """
    for (name, port) in portmap.items():
      if not isinstance(name, Compatibility.string):
        raise ValueError('All portmap keys must be strings!')
      if not isinstance(port, (int, Compatibility.string)):
        raise ValueError('All portmap values must be strings or integers!')

    portmap = portmap.copy()
    for port in list(portmap):
      try:
        portmap[port] = int(portmap[port])
      except ValueError:
        continue

    def resolve_one(static_port):
      visited = set()
      root = portmap[static_port]
      while root in portmap:
        visited.add(root)
        if portmap[root] in visited:
          raise cls.CycleException('Found cycle in portmap!')
        root = portmap[root]
      return root

    return dict((name, resolve_one(name)) for name in portmap)
