#!/usr/bin/env bash
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

# An example scheduler launch script.
# It assumes a local ZooKeeper ensemble running at localhost:2181 and a master under /mesos/master

# Location where aurora-scheduler.zip was unpacked.
AURORA_SCHEDULER_HOME=/usr/local/aurora-scheduler

# Flags that control the behavior of the JVM.
JAVA_OPTS=(
  -server
  -Xmx2g
  -Xms2g

  # Location of libmesos-0.17.0.so / libmesos-0.17.0.dylib
  -Djava.library.path=/usr/local/lib
)

# Flags control the behavior of the Aurora scheduler.
# For a full list of available flags, run bin/aurora-scheduler -help
AURORA_FLAGS=(
  -cluster_name=us-east

  # Ports to listen on.
  -http_port=8081
  -thrift_port=8082

  -native_log_quorum_size=1

  -zk_endpoints=localhost:2181
  -mesos_master_address=zk://localhost:2181/mesos/master

  -serverset_path=/aurora/scheduler

  -native_log_zk_group_path=/aurora/replicated-log

  -native_log_file_path="$AURORA_SCHEDULER_HOME/db"
  -backup_dir="$AURORA_SCHEDULER_HOME/backups"

  # TODO(Kevin Sweeney): Point these to real URLs.
  -thermos_executor_path=/dev/null
  -gc_executor_path=/dev/null

  -vlog=INFO
  -logtostderr
)

# Environment variables control the behavior of the Mesos scheduler driver (libmesos).
export GLOG_v=0
export LIBPROCESS_PORT=8083

JAVA_OPTS="${JAVA_OPTS[*]}" exec "$AURORA_SCHEDULER_HOME/bin/aurora-scheduler" "${AURORA_FLAGS[@]}"
