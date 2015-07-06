#!/bin/bash
#
# aurora Starts the Aurora task scheduler for Mesos.
#
# chkconfig:   345 55 25
# description: This script starts the Aurora service scheduler for Apache Mesos, \
#              used for scheduling and executing long-running tasks and \
#              cron jobs.
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

### BEGIN INIT INFO
# Provides: aurora
# Required-Start:
# Required-Stop:
# Should-Start:
# Should-Stop:
# Default-Start: 3 4 5
# Default-Stop: 0 1 2 6
# Short-Description: Starts the Aurora task scheduler for Mesos.
# Description: Starts the Aurora task scheduler for Mesos.
### END INIT INFO

# Source LSB function library.
. /lib/lsb/init-functions

aurora_user="aurora"
exec="/usr/bin/aurora-scheduler-startup"
prog="aurora"
logdir="/var/log/aurora"
lockfile="/var/run/aurora.lock"
pid_file="/var/run/aurora.pid"
stderr_log="${logdir}/aurora.log"

# Get a sane screen width
[ -z "${COLUMNS:-}" ] && COLUMNS=80
[ -z "${CONSOLETYPE:-}" ] && CONSOLETYPE="$(/sbin/consoletype)"

# Read in init configuration.
if [ -z "${BOOTUP:-}" ]; then
  if [ -f /etc/sysconfig/init ]; then
    . /etc/sysconfig/init
  else
    BOOTUP=color
    RES_COL=60
    MOVE_TO_COL="echo -en \\033[${RES_COL}G"
    SETCOLOR_SUCCESS="echo -en \\033[1;32m"
    SETCOLOR_FAILURE="echo -en \\033[1;31m"
    SETCOLOR_WARNING="echo -en \\033[1;33m"
    SETCOLOR_NORMAL="echo -en \\033[0;39m"
    LOGLEVEL=1
  fi
  if [ "$CONSOLETYPE" = "serial" ]; then
    BOOTUP=serial
    MOVE_TO_COL=
    SETCOLOR_SUCCESS=
    SETCOLOR_FAILURE=
    SETCOLOR_WARNING=
    SETCOLOR_NORMAL=
  fi
fi

function usage {
  err "Starts the Aurora task scheduler for Mesos."
  err "Usage: ${0} (restart|start|stop|status)"
}

function out {
  printf '%s\n' "$*";
}

function msg {
  out "$*" >&2;
}

function err {
  local x=${?};
  msg "$*";
  return $(( ${x} == 0 ? 1 : ${x} ));
}

function echo_success {
  [ "$BOOTUP" = "color" ] && $MOVE_TO_COL
  echo -n "["
  [ "$BOOTUP" = "color" ] && $SETCOLOR_SUCCESS
  echo -n $"  OK  "
  [ "$BOOTUP" = "color" ] && $SETCOLOR_NORMAL
  echo -n "]"
  echo -ne "\r"
  return 0
}

function echo_failure {
  [ "$BOOTUP" = "color" ] && $MOVE_TO_COL
  echo -n "["
  [ "$BOOTUP" = "color" ] && $SETCOLOR_FAILURE
  echo -n $"FAILED"
  [ "$BOOTUP" = "color" ] && $SETCOLOR_NORMAL
  echo -n "]"
  echo -ne "\r"
  return 1
}

# Parse arguments.
ACTION=${1}

# Ensures that action is.
if [ -z ${ACTION} ]; then
  err "ERROR: No action specified."
  usage
  exit -1
fi

start() {
  [ -x ${exec} ] || exit 5
  [ -f ${config} ] || exit 6
  echo -n $"Starting $prog: "
  start_daemon daemonize -u ${aurora_user} -e ${stderr_log} -p ${pid_file} ${exec}
  retval=$?
  [ $retval -eq 0 ] && (echo_success; touch $lockfile) || echo_failure
  echo
  return $retval
}

stop() {
  echo -n $"Stopping $prog: "
  killproc -p ${pid_file} ${exec}
  retval=$?
  [ $retval -eq 0 ] && (echo_success; rm -f $lockfile) || echo_failure
  echo
  return $retval
}

restart() {
  stop
  start
}

reload() {
  restart
}

force_reload() {
  restart
}

rh_status() {
  pid=$(pidofproc -p ${pid_file} ${prog})
  if [ $? -eq 0 ]; then
    echo "${prog} (pid ${pid}) is running..."
    return 0
  else
    if [ -e $lockfile ]; then
      echo "${prog} dead but lockfile exists"
      return 2
    else
      echo "${prog} is stopped"
      return 1
    fi
  fi
}

rh_status_q() {
  rh_status >/dev/null 2>&1
}

# Executes the requested daemon action.
case "${ACTION}" in
  start)
    rh_status_q && rh_status && exit 0
    start
    ;;
  stop)
    stop
    ;;
  status)
    rh_status
    ;;
  restart)
    restart
    ;;
  *)
    err "ERROR: Invalid action specified."
    usage
    exit -3
esac

exit $?
