#!/usr/bin/env bash
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

# Jenkins build script used with builds at http://builds.apache.org
set -eux
date

# Run all Java tests
./gradlew -Pq clean build

# Pre-fetch python dependencies. This is to avoid build flakiness introduced by
# the resolver used in pants.
export PIP_DEFAULT_TIMEOUT=60
mkdir -p third_party
# We omit mesos.native here since we don't actually build or use it in our unit tests.
pip install -d third_party -r <(grep -v mesos.native 3rdparty/python/requirements.txt)

# Run all Python tests
export JUNIT_XML_BASE="$PWD/dist/test-results"
mkdir -p "$JUNIT_XML_BASE"
./pants test.pytest --no-fast src/test/python::

# Run Python style checks
./build-support/python/isort-check
./build-support/python/checkstyle-check src
