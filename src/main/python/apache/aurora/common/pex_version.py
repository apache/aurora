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

from zipfile import BadZipfile

from pex.pex import PexInfo


class UnknownVersion(Exception):
  pass


def pex_version(executable_path, _from_pex=PexInfo.from_pex):
  try:
    properties = _from_pex(executable_path).build_properties
  except (AttributeError, BadZipfile, IOError, OSError):
    raise UnknownVersion

  # Different versions of pants/pex set different keys in the PEX-INFO file. This approach
  # attempts to work regardless of the pants/pex version used.
  return (properties.get('sha', properties.get('revision')),
      properties.get('date', properties.get('datetime')))
