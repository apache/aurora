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

This directory contains all necessary scripting to support the building of Aurora
nightly and release RPMs.  Building and deployment have been tested against the following
Red Hat flavors:

 * CentOS 6/7 on x86_64
 * Fedora 19/20 on x86_64

How to build using Make and rpmbuild
====================================

1. Install the necessary build dependencies via yum-builddep:

```bash
cd build-support/packaging/rpm
sudo yum install -y make rpm-build yum-utils
make srpm
sudo yum-builddep ../../../dist/rpmbuild/SRPMS/*
```

2. Build the RPM via Make.

```bash
make rpm
```

3. After the RPM building process has concluded, RPMs will land here:

```
$AURORA_HOME/dist/rpmbuild/RPMS
```
