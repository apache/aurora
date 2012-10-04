import copy
import getpass
import json
import os
import posixpath
import sys

from pystachio import Empty, Environment, Integer, Ref
from twitter.common import log
from twitter.common.dirutil import safe_open
from twitter.common.lang import Compatibility
from twitter.thermos.config.loader import ThermosTaskValidator
from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import (
  MesosContext,
  MesosJob,
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
    self._job = self.sanitize_job(job)
    self._hdfs_path = None

  @staticmethod
  def sanitize_job(job):
    """
      Do any necessarity sanitation of input job.  Currently we only make
      sure that the maximum process failures is capped at a reasonable
      maximum, 100.
    """
    def process_over_failure_limit(proc):
      return (proc.max_failures() == Integer(0) or proc.max_failures() >= Integer(100))
    for proc in job.task().processes():
      if process_over_failure_limit(proc):
        log.warning('Processes running in Mesos must have failure limits between 1 and 100, '
                    'changing Process(%s) failure limit from %s to 100.' % (proc.name(),
                     proc.max_failures()))
    return job(task = job.task()(
      processes = [proc(max_failures = 100) if process_over_failure_limit(proc) else proc
                   for proc in job.task().processes()]))

  def context(self, instance=None):
    context = dict(
      role=self.role(),
      cluster=self.cluster(),
      package=posixpath.basename(self._hdfs_path) if self._hdfs_path else None,
      package_uri=self._hdfs_path,
      instance=instance
    )
    # Filter unspecified values
    return Environment(mesos = MesosContext(dict((key, val) for key, val in context.items() if val)))

  def job(self):
    interpolated_job = self._job % self.context()
    typecheck = interpolated_job.check()
    if not typecheck.ok():
      raise self.InvalidConfig(typecheck.message())
    return convert_pystachio_to_thrift(interpolated_job)

  def bind(self, binding):
    self._job = self._job.bind(binding)

  def raw(self):
    return self._job

  def task(self, instance):
    return (self._job % self.context(instance)).task()

  def name(self):
    return self._job.name().get()

  def hdfs_path(self):
    return self._hdfs_path

  def set_hdfs_path(self, path):
    if not isinstance(path, Compatibility.string):
      raise ValueError('HDFS uri must be a string')
    self._hdfs_path = path

  def role(self):
    return self._job.role().get()

  def cluster(self):
    return self._job.cluster().get()

  def ports(self):
    return ThermosTaskWrapper(self._job.task(), strict=False).ports()

  def update_config(self):
    return MesosConfig.get_update_config(self._job.get())

  def package(self):
    if self._job.has_package() and self._job.package().check().ok():
      package = self._job.package() % self.context()
      return map(str, [package.role(), package.name(), package.version()])
