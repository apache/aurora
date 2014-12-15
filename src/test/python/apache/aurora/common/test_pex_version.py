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

import mock
import pytest
from twitter.common.python.pex import PexInfo

from apache.aurora.common.pex_version import pex_version, UnknownVersion

SHA = 'foo'
DATE = 'some-date'


def test_old_pants_output():
  mock_pex_info = mock.create_autospec(PexInfo, spec_set=True)
  mock_pex_info.build_properties = {'sha': SHA, 'date': DATE}

  mock_from_pex = mock.create_autospec(PexInfo.from_pex, spec_set=True)
  mock_from_pex.return_value = mock_pex_info

  sha, date = pex_version('path/to/some.pex', _from_pex=mock_from_pex)

  assert sha == SHA
  assert date == DATE


def test_new_pants_output():
  mock_pex_info = mock.create_autospec(PexInfo, spec_set=True)
  mock_pex_info.build_properties = {'revision': SHA, 'datetime': DATE}

  mock_from_pex = mock.create_autospec(PexInfo.from_pex, spec_set=True)
  mock_from_pex.return_value = mock_pex_info

  sha, date = pex_version('path/to/some.pex', _from_pex=mock_from_pex)

  assert sha == SHA
  assert date == DATE


def test_attribute_error():
  mock_from_pex = mock.create_autospec(PexInfo.from_pex, spec_set=True)
  mock_from_pex.side_effect = AttributeError

  with pytest.raises(UnknownVersion):
    pex_version('path/to/some.pex', _from_pex=mock_from_pex)


def test_no_pants():
  mock_from_pex = mock.create_autospec(PexInfo.from_pex, spec_set=True)
  mock_from_pex.side_effect = BadZipfile

  with pytest.raises(UnknownVersion):
    pex_version('path/to/some.pex', _from_pex=mock_from_pex)


def test_io_error():
  mock_from_pex = mock.create_autospec(PexInfo.from_pex, spec_set=True)
  mock_from_pex.side_effect = IOError

  with pytest.raises(UnknownVersion):
    pex_version('path/to/some.pex', _from_pex=mock_from_pex)


def test_os_error():
  mock_from_pex = mock.create_autospec(PexInfo.from_pex, spec_set=True)
  mock_from_pex.side_effect = OSError

  with pytest.raises(UnknownVersion):
    pex_version('path/to/some.pex', _from_pex=mock_from_pex)
