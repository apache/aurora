from twitter.common.lang import Compatibility
from twitter.thermos.config.loader import PortExtractor, ThermosTaskWrapper
from twitter.thermos.config.schema import ThermosContext

from .loader import AuroraConfigLoader
from .schema import MesosContext
from .thrift import convert as convert_thrift

from pystachio import Environment, Integer, Ref


class PortResolver(object):
  class CycleException(Exception): pass

  @classmethod
  def resolve(cls, portmap):
    """
        Given an announce-style portmap, return a fully dereferenced portmap.

        For example, given the portmap:
          {
            'http': 80,
            'aurora: 'http',
            'https': 'aurora',
            'thrift': 'service'
          }

        Returns {'http': 80, 'aurora': 80, 'https': 80, 'thrift': 'service'}
    """
    for (name, port) in portmap.items():
      if not isinstance(name, Compatibility.string):
        raise ValueError('All portmap keys must be strings!')
      if not isinstance(port, (int, Compatibility.string)):
        raise ValueError('All portmap values must be strings or integers!')

    portmap = portmap.copy()
    for port in list(portmap):
      try:
        portmap[port] = int(portmap[port])
      except ValueError:
        continue

    def resolve_one(static_port):
      visited = set()
      root = portmap[static_port]
      while root in portmap:
        visited.add(root)
        if portmap[root] in visited:
          raise cls.CycleException('Found cycle in portmap!')
        root = portmap[root]
      return root

    return dict((name, resolve_one(name)) for name in portmap)

  @classmethod
  def unallocated(cls, portmap):
    """Given a resolved portmap, return the list of ports that need to be allocated."""
    return set(port for port in portmap.values() if not isinstance(port, int))

  @classmethod
  def bound(cls, portmap):
    """Given a resolved portmap, return the list of ports that have already been allocated."""
    return set(portmap)


class AuroraConfig(object):
  class Error(Exception): pass
  class InvalidConfig(Error): pass

  @classmethod
  def pick(cls, env, name, bindings, select_cluster=None, select_env=None):
    job_list = env.get('jobs', [])
    if not job_list:
      raise ValueError('No jobs specified!')

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

    bound_jobs = map(maybe_bind, job_list)
    matches = [j for j in bound_jobs if match_name(j) and match_cluster(j) and match_env(j)]

    if len(matches) == 0:
      msg = 'Could not find job with name %s' % name
      if select_cluster:
        msg = '%s, cluster %s' % (msg, select_cluster)
      if select_env:
        msg = '%s, env %s' % (msg, select_env)
      raise ValueError(msg)
    elif len(matches) > 1:
      raise ValueError('Multiple jobs match, please disambiguate by specifying a cluster or env.')
    else:
      return matches[0]

  @classmethod
  def load(cls, filename, name=None, bindings=None, select_cluster=None, select_env=None):
    env = AuroraConfigLoader.load(filename)
    return cls(cls.pick(env, name, bindings, select_cluster, select_env))

  @classmethod
  def load_json(cls, filename, name=None, bindings=None, select_cluster=None, select_env=None):
    job = AuroraConfigLoader.load_json(filename)
    return cls(job.bind(*bindings) if bindings else job)

  def __init__(self, job):
    self._job = self.sanitize_job(job)
    self._packages = []

  @staticmethod
  def sanitize_job(job):
    """
      Validate and sanitize the input job

      Currently, the validation stage simply ensures that the job has all required fields.
      self.InvalidConfig is raised if any required fields are not present.

      The sanitization ensures that the maximum process failures is capped at a reasonable maximum
      (currently, 100)
    """
    def has(pystachio_type, thing):
      return getattr(pystachio_type, 'has_%s' % thing)()
    for required in ("cluster", "task", "role"):
      if not has(job, required):
        raise AuroraConfig.InvalidConfig(
          '%s required for job "%s"' % (required.capitalize(), job.name()))
    if not has(job.task(), 'processes'):
      raise AuroraConfig.InvalidConfig('Processes required for task on job "%s"' % job.name())

    def process_over_failure_limit(proc):
      return (proc.max_failures() == Integer(0) or proc.max_failures() >= Integer(100))
    return job(task=job.task()(
      processes = [proc(max_failures=100) if process_over_failure_limit(proc) else proc
                   for proc in job.task().processes()]))

  def context(self, instance=None):
    context = dict(
      role=self.role(),
      cluster=self.cluster(),
      instance=instance
    )
    # Filter unspecified values
    return Environment(mesos = MesosContext(dict((k,v) for k,v in context.items() if v)))

  def job(self):
    interpolated_job = self._job % self.context()
    # Typecheck against the Job, with the following free variables unwrapped at the Task level:
    #  - a dummy {{mesos.instance}}
    #  - dummy values for the {{thermos.ports}} context, to allow for their use in task_links
    try:
      dummy_ports = dict(
        (port, 31337) for port in PortExtractor.extract(interpolated_job.task_links()))
    except PortExtractor.InvalidPorts as err:
      raise self.InvalidConfig('Invalid port references in task_links! %s' % err)
    typecheck = interpolated_job.bind(Environment(
        mesos=Environment(instance=0), thermos=ThermosContext(ports=dummy_ports))).check()
    if not typecheck.ok():
      raise self.InvalidConfig(typecheck.message())
    interpolated_job = interpolated_job(task_links=self.task_links())
    return convert_thrift(interpolated_job, self._packages)

  def bind(self, binding):
    self._job = self._job.bind(binding)

  def raw(self):
    return self._job

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

  def ports(self):
    # Strictly speaking this is wrong -- it is possible to do things like
    #   {{thermos.ports[instance_{{mesos.instance}}]}}
    # which can only be extracted post-unwrapping.  This means that validating
    # the state of the announce configuration could be problematic if people
    # try to do complicated things.
    #
    # This should only return the list of ports that need allocation.  In other words
    # we take the ports referenced by processes (referenced_ports), remove the ones
    # that have already been preallocated ("bound") by the portmap, then
    # add the ones that need to be allocated to fulfill their duty in the portmap.
    referenced_ports = ThermosTaskWrapper(self._job.task(), strict=False).ports()
    portmap = PortResolver.resolve(self._job.announce().portmap().get()
                                   if self._job.has_announce() else {})
    return PortResolver.unallocated(portmap) | (referenced_ports - PortResolver.bound(portmap))

  def task_links(self):
    # {{mesos.instance}} --> %shard_id%
    # {{thermos.ports[foo]}} --> %port:foo%
    task_links = self._job.task_links()
    _, uninterp = task_links.interpolate()
    substitutions = {
      Ref.from_address('mesos.instance'): '%shard_id%'
    }
    port_scope = Ref.from_address('thermos.ports')
    for ref in uninterp:
      subscope = port_scope.scoped_to(ref)
      if subscope:
        substitutions[ref] = '%%port:%s%%' % subscope.action().value
    return task_links.bind(substitutions)

  def update_config(self):
    return self._job.update_config()

  def add_package(self, package):
    self._packages.append(package)

  def package(self):
    if self._job.has_package() and self._job.package().check().ok():
      package = self._job.package() % self.context()
      return map(str, [package.role(), package.name(), package.version()])

  def is_dedicated(self):
    return self._job.has_constraints() and 'dedicated' in self._job.constraints()
