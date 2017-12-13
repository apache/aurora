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
./gradlew --no-daemon -Pq clean build

# Run Python import ordering check
./build-support/python/isort-check

# Run remaining Python style checks and all tests
./build-support/python/checkstyle-check
./pants test.pytest --junit-xml-dir="$PWD/dist/test-results" src/test/python:: -- -v

# Ensure we can build python sdists (AURORA-1174)
./build-support/release/make-python-sdists
