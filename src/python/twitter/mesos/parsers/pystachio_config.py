import copy
import getpass
import json
import os
import sys

from pystachio import Empty, Environment, Ref
from twitter.common.dirutil import safe_open
from twitter.common.lang import Compatibility
from twitter.thermos.config.loader import ThermosTaskValidator
from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import (
  MesosContext,
  MesosJob,
  MesosTaskInstance
)

from twitter.thermos.config.loader import ThermosTaskWrapper

from .base import ThriftCodec
from .mesos_config import MesosConfig
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

  # TODO(wickman)  This code smells.  Refactor so that we can share the common include
  # code between Thermos and Mesos, perhaps with a DSLBuilder in Pystachio that has the
  # basic AST parsing + injection of a prescribed environment + 'include' statement(s).
  @staticmethod
  def load(filename, name=None, bindings=None):
    deposit_stack = [os.path.dirname(filename)]
    def ast_executor(config_file, env):
      actual_file = os.path.join(deposit_stack[-1], config_file)
      deposit_stack.append(os.path.dirname(actual_file))
      with open(actual_file) as fp:
        Compatibility.exec_function(compile(fp.read(), actual_file, 'exec'), env)
      deposit_stack.pop()
    def export(*args, **kw):
      pass
    schema_copy = copy.copy(MesosConfigLoader.SCHEMA)
    schema_copy.update(
      mesos_include=lambda fn: ast_executor(fn, schema_copy),
      include=lambda fn: ast_executor(fn, schema_copy),
      export=export)
    ast_executor(os.path.basename(filename), schema_copy)
    job_list = schema_copy.get('jobs', [])
    if not isinstance(job_list, list) or len(job_list) == 0:
      raise MesosConfigLoader.BadConfig("Could not extract any jobs from %s" % filename)
    return MesosConfigLoader.pick(job_list, name, bindings)

  @staticmethod
  def load_json(filename, name=None, bindings=None):
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

  def update_config(self):
    return MesosConfig.get_update_config(self._job.get())
