#!/usr/bin/env python
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
"""Package thermos_runner within thermos_executor."""

import contextlib
import zipfile

with contextlib.closing(zipfile.ZipFile('dist/thermos_executor.pex',
                                        'a')) as zf:
    zf.writestr('apache/aurora/executor/resources/__init__.py', '')
    zf.write('dist/thermos_runner.pex',
             'apache/aurora/executor/resources/thermos_runner.pex')
