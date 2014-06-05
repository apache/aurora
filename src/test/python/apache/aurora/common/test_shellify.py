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

from apache.aurora.common.shellify import shellify


def test_shellify():
  dump = list(shellify(
    {
      "num": 123,
      "string": "abc",
      "obj": {
        "num": 456,
        "string": "def",
        "description": "Runs the world",
      },
      "arr": [
        7,
        "g",
        {
          "hi": [0],
        },
      ]
    }, prefix="TEST_"))

  assert set(dump) == set([
    "TEST_NUM=123",
    "TEST_STRING=abc",
    "TEST_OBJ_NUM=456",
    "TEST_OBJ_STRING=def",
    "TEST_OBJ_DESCRIPTION='Runs the world'",
    "TEST_ARR_0=7",
    "TEST_ARR_1=g",
    "TEST_ARR_2_HI_0=0",
  ])
