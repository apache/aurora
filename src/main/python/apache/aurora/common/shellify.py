#
# Copyright 2014 Apache Software Foundation
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

import pipes

from twitter.common.lang import Compatibility


def shellify(dict_, export=False, prefix=""):
  """Dump a dict to a shell script."""
  if export:
    prefix = "export " + prefix
  def _recurse(k, v, prefix):
    if isinstance(v, bool):
      v = int(v)
    if isinstance(v, int):
      yield "%s=%s" % (prefix + k, + v)
    if isinstance(v, Compatibility.string):
      yield "%s=%s" % (prefix + k, pipes.quote(str(v)))
    elif isinstance(v, dict):
      for k1, v1 in v.items():
        for i in _recurse(k1.upper(), v1, prefix=prefix + k + "_"):
          yield i
    elif isinstance(v, list):
      for k1, v1 in enumerate(v):
        for i in _recurse(str(k1).upper(), v1, prefix=prefix + k + "_"):
          yield i
  for k, v in dict_.items():
    for i in _recurse(k.upper(), v, prefix):
      yield i
