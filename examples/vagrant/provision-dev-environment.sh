#!/bin/bash -x
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

apt-get update
apt-get -y install \
    git automake libtool g++ java7-runtime-headless curl \
    openjdk-7-jdk python-dev libsasl2-dev libcurl4-openssl-dev \
    make

if [ ! -d aurora ]; then
  echo Cloning aurora repo
  git clone /vagrant aurora
fi

pushd aurora
  AURORA_VERSION=$(cat .auroraversion | tr '[a-z]' '[A-Z]')
  mkdir -p third_party
  pushd third_party
    wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.17.0_amd64.egg \
      -O mesos-0.17.0-py2.7-linux-x86_64.egg
  popd
  git pull

  # build scheduler
  ./gradlew distTar

  # build clients
  ./pants src/main/python/apache/aurora/client/bin:aurora_admin
  ./pants src/main/python/apache/aurora/client/bin:aurora_client

  # build executors/observers
  ./pants src/main/python/apache/aurora/executor/bin:gc_executor
  ./pants src/main/python/apache/aurora/executor/bin:thermos_executor
  ./pants src/main/python/apache/aurora/executor/bin:thermos_runner
  ./pants src/main/python/apache/thermos/observer/bin:thermos_observer

  # package runner w/in executor
  python <<EOF
import contextlib
import zipfile
with contextlib.closing(zipfile.ZipFile('dist/thermos_executor.pex', 'a')) as zf:
  zf.writestr('apache/aurora/executor/resources/__init__.py', '')
  zf.write('dist/thermos_runner.pex', 'apache/aurora/executor/resources/thermos_runner.pex')
EOF

  mkdir -p /vagrant/dist/distributions
  cp dist/distributions/aurora-scheduler-$AURORA_VERSION.tar /vagrant/dist/distributions

  for pex in aurora_admin aurora_client gc_executor thermos_executor thermos_observer; do
    cp dist/$pex.pex /vagrant/dist
  done
popd

sudo chown -R vagrant:vagrant aurora
