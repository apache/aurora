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

source_root('src/main/python', Page, PythonBinary, PythonLibrary)
source_root('src/main/thrift', Page, PythonLibrary, PythonThriftLibrary)

# TODO(wickman) get rid of PythonLibrary from src/test/python:
#   src/test/python/apache/aurora/client/BUILD:python_library(
#   src/test/python/apache/aurora/client/cli/BUILD:python_library(
#   src/test/python/apache/aurora/client/commands/BUILD:python_library(
source_root('src/test/python', Page, PythonTests, PythonTestSuite, PythonLibrary)
