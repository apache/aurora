#!/bin/bash -ex
# TODO(ksweeney): Use public and versioned URLs instead of local artifacts.
tar xvf /vagrant/dist/distributions/aurora-scheduler.tar -C /usr/local
install -m 755 /vagrant/dist/aurora_client.pex /usr/local/bin/aurora
install -m 755 /vagrant/dist/aurora_admin.pex /usr/local/bin/aurora_admin

apt-get update
apt-get -y install java7-runtime-headless curl
wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.15.0-rc4_amd64.deb
dpkg --install mesos_0.15.0-rc4_amd64.deb

# TODO(ksweeney): Make this a be part of the Aurora distribution tarball.
cat > /usr/local/sbin/aurora-scheduler.sh <<"EOF"
#!/usr/bin/env bash
# An example scheduler launch script that works with the included Vagrantfile.

# Location where aurora-scheduler.zip was unpacked.
AURORA_SCHEDULER_HOME=/usr/local/aurora-scheduler

# Flags that control the behavior of the JVM.
JAVA_OPTS=(
  -server
  -Xmx1g
  -Xms1g

  # Location of libmesos-0.15.0.so / libmesos-0.15.0.dylib
  -Djava.library.path=/usr/local/lib
)

# Flags control the behavior of the Aurora scheduler.
# For a full list of available flags, run bin/aurora-scheduler -help
AURORA_FLAGS=(
  -cluster_name=example

  # Ports to listen on.
  -http_port=8081
  -thrift_port=8082

  -native_log_quorum_size=1

  -zk_endpoints=192.168.33.2:2181
  -mesos_master_address=zk://192.168.33.2:2181/mesos/master

  -serverset_path=/aurora/scheduler

  -native_log_zk_group_path=/aurora/replicated-log

  -native_log_file_path="$AURORA_SCHEDULER_HOME/db"
  -backup_dir="$AURORA_SCHEDULER_HOME/backups"

  -thermos_executor_path=/usr/local/bin/thermos_executor
  -gc_executor_path=/usr/local/bin/gc_executor

  -vlog=INFO
  -logtostderr
)

# Environment variables control the behavior of the Mesos scheduler driver (libmesos).
export GLOG_v=0
export LIBPROCESS_PORT=8083
export LIBPROCESS_IP=192.168.33.5

(
  while true
  do
    JAVA_OPTS="${JAVA_OPTS[*]}" exec "$AURORA_SCHEDULER_HOME/bin/aurora-scheduler" "${AURORA_FLAGS[@]}"
  done
) &
EOF
chmod +x /usr/local/sbin/aurora-scheduler.sh

mkdir -p /etc/aurora
cat > /etc/aurora/clusters.json <<EOF
[{
  "name": "example",
  "zk": "192.168.33.2",
  "scheduler_zk_path": "/aurora/scheduler",
  "auth_mechanism": "UNAUTHENTICATED"
}]
EOF

cat > /etc/rc.local <<EOF
#!/bin/sh -e
/usr/local/sbin/aurora-scheduler.sh \
  1> /var/log/aurora-scheduler-stdout.log \
  2> /var/log/aurora-scheduler-stderr.log
EOF
chmod +x /etc/rc.local

/etc/rc.local
