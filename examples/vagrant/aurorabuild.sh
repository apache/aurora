#!/bin/bash -e
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

# A script to update sources within the vagrant environment an rebuild/install aurora components.
# Usage: aurorabuild [COMPONENT]...
# where COMPONENT is a name for an aurora component that makes up part of the infrastructure.
# Run with zero arguments for a full list of components that may be built.

set -o nounset

REPO_DIR=/home/vagrant/aurora
DIST_DIR=$REPO_DIR/dist

function upstart_update {
  # Stop and start is necessary to update a the configuration of
  # an upstart job.  We'll rarely change the configuration, but
  # it's probably better to do this upfront and avoid surprises/confusion.
  # Executing true on failure to please bash -e
  sudo stop $1  || true
  sudo start $1 || true
}

function build_client {
  ./pants binary src/main/python/apache/aurora/kerberos:kaurora
  sudo ln -sf $DIST_DIR/kaurora.pex /usr/local/bin/aurora
}

function build_admin_client {
  ./pants binary src/main/python/apache/aurora/kerberos:kaurora_admin
  sudo ln -sf $DIST_DIR/kaurora_admin.pex /usr/local/bin/aurora_admin
}

function build_scheduler {
  # This CLASSPATH_PREFIX is inserted at the front of the CLASSPATH to enable "hot" reloads of the
  # UI code (c.f. the startScripts task in build.gradle).
  # NB: We limit the scope of the hot-reloadable resources to the UI resources in the /schedulder
  # top-level resource dir; ie we do not support hot-loading of other resources like /logback.xml
  # or /org/apache/aurora/scheduler/storage/db/schema.sql
  local hot_resources_dir=/vagrant/dist/.hot_resources
  if [ ! -L $hot_resources_dir/scheduler ]
  then
    rm -rf $hot_resources_dir && mkdir -p $hot_resources_dir
    ln -s /vagrant/dist/resources/main/scheduler $hot_resources_dir/scheduler
  fi
  CLASSPATH_PREFIX=$hot_resources_dir ./gradlew installDist

  sudo mkdir -p /var/db/aurora
  if sudo mesos-log initialize --path="/var/db/aurora"
  then
    echo "Replicated log initialized."
  else
    echo "Replicated log initialization failed with code $? (likely already initialized)."
  fi
  upstart_update aurora-scheduler
}

function build_executor {
  ./pants binary src/main/python/apache/aurora/executor:thermos_executor
  ./pants binary src/main/python/apache/thermos/runner:thermos_runner

  # Package runner within executor.
  build-support/embed_runner_in_executor.py

  chmod +x /home/vagrant/aurora/dist/thermos_executor.pex
}

function build_observer {
  ./pants binary src/main/python/apache/aurora/tools:thermos_observer
  ./pants binary src/main/python/apache/aurora/tools:thermos
  sudo ln -sf $DIST_DIR/thermos.pex /usr/local/bin/thermos
  upstart_update aurora-thermos-observer
}

function build_all {
  build_admin_client
  build_client
  build_executor
  build_observer
  build_scheduler
}

function print_components {
  echo 'Please select from: admin_client, client, executor, observer, scheduler or all.'
}

if [ "$#" -eq 0 ]
then
  echo 'Must specify at least one component to build'
  print_components
  exit 1
fi

for component in "$@"
do
  type "build_$component" >/dev/null && continue
  echo "Component $component is unrecognized."
  print_components
  exit 1
done

cd $REPO_DIR
update-sources
for component in "$@"
do
  build_$component
done

exit 0
