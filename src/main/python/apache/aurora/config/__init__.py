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

from __future__ import absolute_import

from collections import defaultdict

from pystachio import Empty, Environment, Ref

from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.config.schema.base import MesosContext
from apache.thermos.config.loader import ThermosTaskWrapper

from .loader import AuroraConfigLoader
from .port_resolver import PortResolver
from .thrift import InvalidConfig as InvalidThriftConfig
from .thrift import convert as convert_thrift

__all__ = ('AuroraConfig', 'PortResolver')


class AuroraConfig(object):
  class Error(Exception): pass

  class InvalidConfig(Error):
    def __str__(self):
      return 'The configuration was invalid: %s' % self.args[0]

  @classmethod
  def plugins(cls):
    """A stack of callables to apply to the config on load."""
    return []

  @classmethod
  def pick(cls, env, name, bindings, select_cluster=None, select_role=None, select_env=None):
    # TODO(atollenaere): should take a JobKey when non-jobkey interface is deprecated

    job_list = env.get('jobs', [])
    if not job_list:
      raise ValueError('No job defined in this config!')

    def maybe_bind(j):
      return j.bind(*bindings) if bindings else j

    if name is None:
      if len(job_list) > 1:
        raise ValueError('Configuration has multiple jobs but no job name specified!')
      return maybe_bind(job_list[0])

    # TODO(wfarner): Rework this and calling code to make name optional as well.
    def match_name(job):
      return str(job.name()) == name
    def match_cluster(job):
      return select_cluster is None or str(job.cluster()) == select_cluster
    def match_env(job):
      return select_env is None or str(job.environment()) == select_env
    def match_role(job):
      return select_role is None or str(job.role()) == select_role

    bound_jobs = map(maybe_bind, job_list)
    matches = [j for j in bound_jobs if
               all([match_cluster(j), match_role(j), match_env(j), match_name(j)])]

    if len(matches) == 0:
      msg = "Could not find job %s/%s/%s/%s\n" % (
        select_cluster or '*', select_role or '*', select_env or '*', name)
      for j in bound_jobs:
        if j.environment() is Empty:
          msg += "Job %s/%s/%s/%s in configuration file doesn't specify an environment\n" % (
            j.cluster(), j.role(), '{MISSING}', j.name()
          )
      msg += cls._candidate_jobs_str(bound_jobs)
      raise ValueError(msg)

    elif len(matches) > 1:
      msg = 'Multiple jobs match, please disambiguate by specifying a job key.\n'
      msg += cls._candidate_jobs_str(bound_jobs)
      raise ValueError(msg)
    else:
      return matches[0]

  @staticmethod
  def _candidate_jobs_str(job_list):
    assert(job_list)
    job_list = ["  %s/%s/%s/%s" % (
        j.cluster(), j.role(),
        j.environment() if j.environment() is not Empty else "{MISSING}",
        j.name())
        for j in job_list]
    return 'Candidates are:\n' + '\n'.join(job_list)

  @classmethod
  def apply_plugins(cls, config, env=None):
    for plugin in cls.plugins():
      if not callable(plugin):
        raise cls.Error('Invalid configuration plugin %r, should be callable!' % plugin)
      plugin(config, env)
    return config

  @classmethod
  def load(
        cls, filename, name=None, bindings=None,
        select_cluster=None, select_role=None, select_env=None):
    # TODO(atollenaere): should take a JobKey when non-jobkey interface is deprecated
    env = AuroraConfigLoader.load(filename)
    return cls.apply_plugins(
        cls(cls.pick(env, name, bindings, select_cluster, select_role, select_env)), env)

  @classmethod
  def load_json(
        cls, filename, name=None, bindings=None,
        select_cluster=None, select_role=None, select_env=None):
    # TODO(atollenaere): should take a JobKey when non-jobkey interface is deprecated
    env = AuroraConfigLoader.load_json(filename)
    return cls.apply_plugins(
        cls(cls.pick(env, name, bindings, select_cluster, select_role, select_env)), env)

  @classmethod
  def loads_json(
        cls, string, name=None, bindings=None,
        select_cluster=None, select_role=None, select_env=None):
    # TODO(atollenaere): should take a JobKey when non-jobkey interface is deprecated
    env = AuroraConfigLoader.loads_json(string)
    return cls.apply_plugins(
        cls(cls.pick(env, name, bindings, select_cluster, select_role, select_env)), env)

  @classmethod
  def validate_job(cls, job):
    """
      Validate and sanitize the input job

      Currently, the validation stage simply ensures that the job has all required fields.
      self.InvalidConfig is raised if any required fields are not present.
    """
    def has(pystachio_type, thing):
      return getattr(pystachio_type, 'has_%s' % thing)()
    for required in ("cluster", "task", "role"):
      if not has(job, required):
        raise cls.InvalidConfig(
          '%s required for job "%s"' % (required.capitalize(), job.name()))
    if not has(job.task(), 'processes'):
      raise cls.InvalidConfig('Processes required for task on job "%s"' % job.name())

  @classmethod
  def standard_bindings(cls, job):
    # Rewrite now-deprecated bindings into their proper form.
    return job.bind({
      Ref.from_address('mesos.role'): '{{role}}',
      Ref.from_address('mesos.cluster'): '{{cluster}}',
      Ref.from_address('thermos.user'): '{{role}}',
    })

  def __init__(self, job):
    self.validate_job(job)  # first-pass validation that required fields are present
    self._job = self.standard_bindings(job)
    self._metadata = []
    self.binding_dicts = defaultdict(dict)
    self.hooks = []

  def context(self, instance=None):
    context = dict(instance=instance)
    # Filter unspecified values
    return Environment(mesos=MesosContext(dict((k, v) for k, v in context.items() if v)))

  def job(self):
    interpolated_job = self._job % self.context()
    try:
      return convert_thrift(interpolated_job, self._metadata, self.ports())
    except InvalidThriftConfig as e:
      raise self.InvalidConfig(str(e))

  def bind(self, binding):
    self._job = self._job.bind(binding)

  def raw(self):
    return self._job

  # This stinks to high heaven
  def update_job(self, new_job):
    self._job = new_job

  def instances(self):
    return self._job.instances().get()

  def task(self, instance):
    return (self._job % self.context(instance)).task()

  def name(self):
    return self._job.name().get()

  def role(self):
    return self._job.role().get()

  def cluster(self):
    return self._job.cluster().get()

  def environment(self):
    return self._job.environment().get()

  def job_key(self):
    return AuroraJobKey(self.cluster(), self.role(), self.environment(), self.name())

  def ports(self):
    """Return the list of ports that need to be allocated by the scheduler."""

    # Strictly speaking this is wrong -- it is possible to do things like
    #   {{thermos.ports[instance_{{mesos.instance}}]}}
    # which can only be extracted post-unwrapping.  This means that validating
    # the state of the announce configuration could be problematic if people
    # try to do complicated things.
    referenced_ports = ThermosTaskWrapper(self._job.task(), strict=False).ports()
    resolved_portmap = PortResolver.resolve(self._job.announce().portmap().get()
                                            if self._job.has_announce() else {})

    # values of the portmap that are not integers => unallocated
    unallocated = set(port for port in resolved_portmap.values() if not isinstance(port, int))

    # find referenced {{thermos.portmap[ports]}} that are not resolved by the portmap
    unresolved_references = set(
      port for port in (resolved_portmap.get(port_ref, port_ref) for port_ref in referenced_ports)
      if not isinstance(port, int))

    return unallocated | unresolved_references

  def has_health_port(self):
    return "health" in ThermosTaskWrapper(self._job.task(), strict=False).ports()

  def update_config(self):
    return self._job.update_config()

  def health_check_config(self):
    return self._job.health_check_config()

  def add_metadata(self, key, value):
    self._metadata.append((key, value))

  def is_dedicated(self):
    return self._job.has_constraints() and 'dedicated' in self._job.constraints()

  def __repr__(self):
    return '%s(%r)' % (self.__class__.__name__, self._job)
