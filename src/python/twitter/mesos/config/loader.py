import copy
import json
import sys

from pystachio import Ref
from twitter.common.dirutil import safe_open
from twitter.common.lang.compatibility import *
from twitter.mesos.config.schema import MesosTaskInstance
from twitter.thermos.config.loader import ThermosTaskWrapper

SCHEMA_PREAMBLE = """
from pystachio import *
from twitter.mesos.config.schema import *
from twitter.thermos.config.dsl import *
"""

def deposit_schema(environment):
  exec_function(compile(SCHEMA_PREAMBLE, "<exec_function>", "exec"), environment)


class MesosJobWrapper(object):

  class InvalidJobError(Exception): pass

  def __init__(self, job):
    if not job.check().ok():
      raise MesosJobWrapper.InvalidJobError("Invalid job format: %s" % job.check().message())

    self._job = job

  def job(self):
    return self._job

  def task(self):
    """Returns the underlying  (wrapped) task"""
    return ThermosTaskWrapper(self._job.task())

  def task_instance(self, num_instance):
    """Returns the mesos task instance for this task"""
    job = self._job

    ti = MesosTaskInstance(task=job.task(),
                           layout=job.layout(),
                           role=job.role(),
                           instance=num_instance)

    assert ti.check().ok(), "Invalid task instance %s" % ti.check().message()
    return ti

  def task_instance_json(self, num_instance):
    """Returns the json dump of the mesos task instance"""
    return json.dumps(self.task_instance(num_instance).get())

  def ports(self):
    """Return the requested ports"""
    return self.task().ports()

  def validate(self):
    # TODO(vinod): Implement this
    return True


class MesosConfigLoader(object):
  SCHEMA = {}
  deposit_schema(SCHEMA)

  class BadConfig(Exception): pass

  @staticmethod
  def load(filename):
    tc = MesosConfigLoader()
    schema_copy = copy.copy(MesosConfigLoader.SCHEMA)
    with open(filename) as fp:
      locals_map = exec_function(compile(fp.read(), filename, 'exec'), schema_copy)

    job_list = None
    try:
      job_list = locals_map['jobs']
    except NameError:
      pass

    if not job_list or not isinstance(job_list, list):
      raise MesosConfigLoader.BadConfig(
        "Could not extract any jobs from %s" % filename)

    for job in job_list:
      tc._add_job(job)

    return tc

  def __init__(self):
    self._jobs = []

  def _add_job(self, job):
    self._jobs.append(MesosJobWrapper(job))

  def jobs(self):
    """Returns the list of (wrapped) jobs."""
    return self._jobs

  def job(self, name):
    """
    Returns the (wrapped) job corresponding to the specified name.
    If no such job exists returns None.
    """
    for mj in self._jobs:
      if mj.job().name().get() == name:
        return mj

    return None
