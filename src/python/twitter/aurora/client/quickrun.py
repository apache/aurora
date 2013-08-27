from __future__ import print_function

from collections import defaultdict
import getpass
import hashlib
import sys
import tempfile
import threading
import time

from twitter.common.contextutil import open_zip
from twitter.common.quantity import Amount, Time, Data

from twitter.aurora.client.config import populate_namespaces
from twitter.aurora.common.aurora_job_key import AuroraJobKey
from twitter.aurora.config.schema import (
    Announcer,
    Constraint,
    Job,
    Packer,
    Process,
    Resources,
    SequentialTask)
from twitter.aurora.config import AuroraConfig
from twitter.packer import sd_packer_client

from gen.twitter.aurora.ttypes import ResponseCode, ScheduleStatus

from .job_monitor import JobMonitor


class Quickrun(object):
  QUERY_INTERVAL = Amount(30, Time.SECONDS)
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
  PACKAGE_NAME = '__quickrun'

  def __init__(self,
               cluster_name,
               command,
               name,
               role=None,
               instances=1,
               cpu=1,
               ram=Amount(1, Data.GB),
               disk=Amount(1, Data.GB),
               announce=False,
               packages=None,
               additional_files=None):
    self._jobname = name
    self._role = role or getpass.getuser()
    self._stop = threading.Event()
    self._instances = instances
    self._cluster_name = cluster_name
    processes = [Process(name=name, cmdline=command)]
    if packages:
      if not isinstance(packages, list):
        raise ValueError('Expect packages to be a list of 3-tuples!')
      for package in packages:
        if not hasattr(package, '__iter__') or len(package) != 3:
          raise ValueError('Expect each package to be a 3-tuple of role, name, version')
        package_role, package_name, package_version = package
        processes.insert(0, Packer.copy(package_name, role=package_role, version=package_version))
    if additional_files:
      processes.insert(0, self.additional_files_process(cluster_name, self._role, additional_files))
    task = SequentialTask(
        name=name,
        processes=processes,
        resources=Resources(cpu=cpu, ram=ram.as_(Data.BYTES), disk=disk.as_(Data.BYTES)))
    job = Job(task=task, instances=instances, role=self._role, environment='test', cluster=cluster_name)
    if announce:
      job = job(announce=Announcer(), daemon=True)
    self._config = self.populate_namespaces(AuroraConfig(job))

  def populate_namespaces(self, config):
    return populate_namespaces(config)

  @classmethod
  def generate_package_file(cls, filemap):
    filename = tempfile.mktemp(suffix='.zip')
    with open_zip(filename, 'w') as zf:
      for an, fn in filemap.items():
        zf.write(fn, arcname=an)
    return filename

  @classmethod
  def get_md5(cls, filename):
    with open(filename) as fp:
      return hashlib.md5(fp.read()).hexdigest()

  @classmethod
  def get_package_file(cls, cluster, role, filename):
    md5 = cls.get_md5(filename)
    packer = sd_packer_client.create_packer(cluster)
    if cls.PACKAGE_NAME in packer.list_packages(role):
      for package in packer.list_versions(role, cls.PACKAGE_NAME):
        if package['md5sum'] == md5:
          return (role, cls.PACKAGE_NAME, package['id'])
    # packer miss, upload new copy
    package = packer.add(role, cls.PACKAGE_NAME, filename, None, digest=md5)
    return (role, cls.PACKAGE_NAME, package['id'])

  @classmethod
  def additional_files_process(cls, cluster, role, filemap):
    # filemap is of the form local => remote filename
    package_role, package_name, package_version = cls.get_package_file(cluster, role,
        cls.generate_package_file(filemap))
    return Packer.install(package_name, role=package_role, version=package_version)

  def _terminal(self, statuses):
    terminals = sum(status in self.FINISHED_STATES for status in statuses.values())
    return terminals == self._instances

  def _write_line(self, line):
    whitespace = max(0, 80 - len(line))
    sys.stderr.write(line + ' ' * whitespace)
    sys.stderr.write('\r')
    sys.stderr.flush()

  def run(self, client):
    monitor = JobMonitor(client, self._config.role(), self._config.environment(), self._config.name())
    response = client.create_job(self._config)
    if response.responseCode != ResponseCode.OK:
      print('Failed to create job: %s' % response.message)
      return False
    try:
      while True:
        statuses = monitor.states()
        statuses_count = defaultdict(int)
        for status in statuses.values():
          statuses_count[status] += 1
        self._write_line(' :: '.join('%s %2d' % (ScheduleStatus._VALUES_TO_NAMES[status], count)
            for (status, count) in statuses_count.items()))
        if self._terminal(statuses):
          print('\nTask finished.')
          return True
        time.sleep(self.QUERY_INTERVAL.as_(Time.SECONDS))
    except KeyboardInterrupt:
      print('\nKilling job...')
      job_key = AuroraJobKey(self._cluster_name, self._config.role(), self._config.environment(),
          self._config.name())
      response = client.kill_job(job_key)
      if response.responseCode != ResponseCode.OK:
        print('Failed to kill task: %s' % response.message)

  def stop(self):
    self._stop.set()

  def __str__(self):
    return str(self._config._job)
