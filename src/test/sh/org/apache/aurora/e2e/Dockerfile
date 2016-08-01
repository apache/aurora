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

FROM python:2.7

# The mesos containerizer does not auto-create mount points, so we must initialize them manually.
# TODO(jcohen): Remove this mkdir when https://issues.apache.org/jira/browse/MESOS-5229 is resolved.
RUN mkdir -p /home/vagrant/aurora/examples/vagrant/config

COPY http_example.py /tmp/
