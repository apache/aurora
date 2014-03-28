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
apt-get -y install java7-runtime-headless curl
wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.17.0_amd64.deb
dpkg --install mesos_0.17.0_amd64.deb
cat > /usr/local/sbin/mesos-master.sh <<EOF
#!/bin/bash
export LD_LIBRARY_PATH=/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server
(
  while true
  do
    /usr/local/sbin/mesos-master --zk=zk://192.168.33.2:2181/mesos/master --ip=192.168.33.3
    echo "Master exited with $?, restarting."
  done
) & disown
EOF
chmod +x /usr/local/sbin/mesos-master.sh

cat > /etc/rc.local <<EOF
#!/bin/sh -e
/usr/local/sbin/mesos-master.sh >/var/log/mesos-master-stdout.log 2>/var/log/mesos-master-stderr.log
EOF
chmod +x /etc/rc.local

/etc/rc.local
