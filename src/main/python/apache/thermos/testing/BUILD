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

python_library(
  name = 'runner',
  sources = ['runner.py'],
  dependencies = [
    pants('3rdparty/python:thrift'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/config'),
    pants('src/main/python/apache/thermos/core'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)
