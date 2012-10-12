from __future__ import print_function

from collections import defaultdict
from functools import partial
import socket
import sys
import threading
import time

from twitter.common import log
from twitter.common.quantity import Amount, Time, Data
from twitter.mesos.client.client_util import populate_namespaces
from twitter.mesos.config.schema import (
    Constraint,
    Job,
    Process,
    Resources,
    Task)
from twitter.mesos.parsers.pystachio_config import PystachioConfig
from gen.twitter.mesos.ttypes import (
   Identity,
   ResponseCode,
   ScheduleStatus,
   TaskQuery)

from thrift.transport import TTransport


class Quickrun(object):
  QUERY_INTERVAL = Amount(5, Time.SECONDS)
  WAIT_STATES = frozenset([
      ScheduleStatus.PENDING,
      ScheduleStatus.ASSIGNED,
      ScheduleStatus.STARTING])
  ACTIVE_STATES = frozenset([
      ScheduleStatus.RUNNING])
  FINISHED_STATES = frozenset([
      ScheduleStatus.FAILED,
      ScheduleStatus.FINISHED,
      ScheduleStatus.KILLED])

  def __init__(self, cluster, command, options):
    self._active_query = TaskQuery(owner=Identity(role=options.role),
        jobName=options.name)
    self._stop = threading.Event()
    self._config = self.config(cluster, command, options)
    self._instances = options.instances

  def config(self, cluster, command, options):
    processes = [Process(name=options.name, cmdline=command)]
    if options.package:
      processes.insert(0, Process(name='installer',
          cmdline='{{packer[%s][%s][%s].copy_command}}' % options.package))
    task = Task(
        name=options.name,
        processes=processes,
        constraints=[Constraint(order=[p.name() for p in processes])],
        resources=Resources(cpu=options.cpus,
                            ram=options.ram.as_(Data.BYTES),
                            disk=options.disk.as_(Data.BYTES)))
    job = Job(task=task, instances=options.instances, role=options.role, cluster=cluster)
    if options.announce:
      job = job(announce=Announcer(environment='test'), daemon=True)
    return populate_namespaces(PystachioConfig(job))

  def _iter_active_slaves(self, client):
    try:
      res = client.scheduler.getTasksStatus(self._active_query)
    except TTransport.TTransportException as e:
      print('Failed to query slaves from scheduler: %s' % e)
      return
    if res is None or res.tasks is None:
      return
    for task in res.tasks:
      yield (task.assignedTask.task.shardId, task.status, task.assignedTask.slaveId)

  def _terminal(self, statuses):
    terminals = sum([count for (status, count) in statuses.items()
                     if status in self.FINISHED_STATES])
    return terminals == self._instances

  def _write_line(self, line):
    whitespace = max(0, 80 - len(line))
    sys.stderr.write(line + ' ' * whitespace)
    sys.stderr.write('\r')
    sys.stderr.flush()

  def run(self, client):
    response = client.create_job(self._config)
    if response.responseCode != ResponseCode.OK:
      print('Failed to create job: %s' % response.message)
      return False
    try:
      while True:
        statuses = defaultdict(int)
        for shard_id, status, slave_id in self._iter_active_slaves(client):
          statuses[status] += 1
        self._write_line(' :: '.join('%s %2d' % (ScheduleStatus._VALUES_TO_NAMES[status], count)
            for (status, count) in statuses.items()))
        if self._terminal(statuses):
          print('\nTask finished.')
          return True
        time.sleep(self.QUERY_INTERVAL.as_(Time.SECONDS))
    except KeyboardInterrupt:
      print('\nKilling job...')
      response = client.kill_job(self._config.role(), self._config.name())
      if response.responseCode != ResponseCode.OK:
        print('Failed to kill task: %s' % response.message)

  def stop(self):
    self._stop.set()

  def __str__(self):
    return str(self._config._job)
