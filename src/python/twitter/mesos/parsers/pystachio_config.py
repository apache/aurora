import copy
import getpass
import json
import sys

from pystachio import Empty, Environment, Ref
from twitter.common.dirutil import safe_open
from twitter.common.lang import Compatibility
from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import (
  MesosContext,
  MesosJob,
  MesosTaskInstance
)

from twitter.thermos.config.loader import ThermosTaskWrapper

from .base import ThriftCodec
from .proxy_config import ProxyConfig
from .pystachio_thrift import convert as convert_pystachio_to_thrift


SCHEMA_PREAMBLE = """
from pystachio import *
from twitter.mesos.config.schema import *
"""

def deposit_schema(environment):
  Compatibility.exec_function(
    compile(SCHEMA_PREAMBLE, "<exec_function>", "exec"), environment)


class MesosConfigLoader(object):
  SCHEMA = {}
  deposit_schema(SCHEMA)

  class BadConfig(Exception): pass

  @staticmethod
  def pick(job_list, name, bindings):
    if not job_list:
      raise ValueError('No jobs specified!')
    if name is None:
      if len(job_list) > 1:
        raise ValueError('Configuration has multiple jobs but no job name specified!')
      return job_list[0].bind(*bindings) if bindings else job_list[0]
    for job in job_list:
      if str(job.name()) == name:
        return job.bind(*bindings) if bindings else job
    raise ValueError('Could not find job named %s!' % name)

  @staticmethod
  def load(filename, name=None, bindings=None):
    tc = MesosConfigLoader()
    schema_copy = copy.copy(MesosConfigLoader.SCHEMA)
    with open(filename) as fp:
      locals_map = Compatibility.exec_function(
        compile(fp.read(), filename, 'exec'), schema_copy)
    job_list = locals_map.get('jobs')
    if not job_list or not isinstance(job_list, list):
      raise MesosConfigLoader.BadConfig("Could not extract any jobs from %s" % filename)
    return MesosConfigLoader.pick(job_list, name, bindings)

  @staticmethod
  def load_json(filename, name=None, bindings=None):
    tc = MesosConfigLoader()
    with open(filename) as fp:
      js = json.load(fp)
    job = MesosJob(js)
    return job.bind(*bindings) if bindings else job


class PystachioConfig(ProxyConfig):
  @staticmethod
  def load(filename, name=None, bindings=None):
    return PystachioConfig(MesosConfigLoader.load(filename, name, bindings))

  @staticmethod
  def load_json(filename, name=None, bindings=None):
    return PystachioConfig(MesosConfigLoader.load_json(filename, name, bindings))

  def __init__(self, job):
    self._job = job

  def job(self):
    return convert_pystachio_to_thrift(self._job)

  def name(self):
    return str(self._job.name())

  def hdfs_path(self):
    return None

  def role(self):
    return self._job.role().get()

  def cluster(self):
    if self._job.cluster().check().ok():
      return self._job.cluster().get()
    else:
      return None

  def ports(self):
    return ThermosTaskWrapper(self._job.task(), strict=False).ports()
