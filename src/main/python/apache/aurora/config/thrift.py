import getpass
import re

from apache.aurora.config.schema.base import (
  HealthCheckConfig,
  MesosContext,
  MesosTaskInstance,
)
from twitter.common.lang import Compatibility
from apache.thermos.config.loader import ThermosTaskValidator

from gen.apache.aurora.constants import GOOD_IDENTIFIER_PATTERN_PYTHON, AURORA_EXECUTOR_NAME
from gen.apache.aurora.ttypes import (
  Constraint,
  CronCollisionPolicy,
  ExecutorConfig,
  Identity,
  JobConfiguration,
  JobKey,
  LimitConstraint,
  Package,
  TaskConfig,
  TaskConstraint,
  ValueConstraint,
)

from pystachio import Empty, Ref

__all__ = (
  'InvalidConfig',
  'convert'
)


class InvalidConfig(ValueError):
  pass


def constraints_to_thrift(constraints):
  """Convert a python dictionary to a set of Constraint thrift objects."""
  result = set()
  for attribute, constraint_value in constraints.items():
    assert isinstance(attribute, Compatibility.string) and (
           isinstance(constraint_value, Compatibility.string)), (
      "Both attribute name and value in constraints must be string")
    constraint = Constraint()
    constraint.name = attribute
    task_constraint = TaskConstraint()
    if constraint_value.startswith('limit:'):
      task_constraint.limit = LimitConstraint()
      try:
        task_constraint.limit.limit = int(constraint_value.replace('limit:', '', 1))
      except ValueError:
        print('%s is not a valid limit value, must be integer' % constraint_value)
        raise
    else:
      # Strip off the leading negation if present.
      negated = constraint_value.startswith('!')
      if negated:
        constraint_value = constraint_value[1:]
      task_constraint.value = ValueConstraint(negated, set(constraint_value.split(',')))
    constraint.constraint = task_constraint
    result.add(constraint)
  return result


def task_instance_from_job(job, instance):
  instance_context = MesosContext(instance=instance)
  # TODO(Sathya): Remove health_check_interval_secs references after deprecation cycle is complete.
  health_check_config = HealthCheckConfig()
  if job.has_health_check_interval_secs():
    health_check_config = HealthCheckConfig(interval_secs=job.health_check_interval_secs().get())
  elif job.has_health_check_config():
    health_check_config = job.health_check_config()
  ti = MesosTaskInstance(task=job.task(),
                         layout=job.layout(),
                         role=job.role(),
                         health_check_interval_secs=health_check_config.interval_secs().get(),
                         health_check_config=health_check_config,
                         instance=instance)
  if job.has_announce():
    ti = ti(announce=job.announce())
  if job.has_environment():
    ti = ti(environment=job.environment())
  return ti.bind(mesos=instance_context).interpolate()


def translate_cron_policy(policy):
  cron_policy = CronCollisionPolicy._NAMES_TO_VALUES.get(policy.get())
  if cron_policy is None:
    raise InvalidConfig('Invalid cron policy: %s' % policy.get())
  return cron_policy


def fully_interpolated(pystachio_object, coerce_fn=lambda i: i):
  # Extract a fully-interpolated unwrapped object from pystachio_object or raise InvalidConfig.
  #
  # TODO(ksweeney): Remove this once Pystachio 1.0 changes the behavior of interpolate() to return
  # unwrapped objects and fail when there are any unbound refs.
  if not pystachio_object.check().ok():
    raise InvalidConfig(pystachio_object.check().message())

  # If an object type-checks it's okay to use the raw value from the wrapped object returned by
  # interpolate. Without the previous check value.get() could return a string with mustaches
  # instead of an object of the expected type.
  value, _ = pystachio_object.interpolate()
  return coerce_fn(value.get())


def select_cron_policy(cron_policy, cron_collision_policy):
  if cron_policy is Empty and cron_collision_policy is Empty:
    return CronCollisionPolicy.KILL_EXISTING
  elif cron_policy is not Empty and cron_collision_policy is Empty:
    return translate_cron_policy(cron_policy)
  elif cron_policy is Empty and cron_collision_policy is not Empty:
    return translate_cron_policy(cron_collision_policy)
  else:
    raise InvalidConfig('Specified both cron_policy and cron_collision_policy!')


def select_service_bit(job):
  if not job.has_daemon() and not job.has_service():
    return False
  elif job.has_daemon() and not job.has_service():
    return fully_interpolated(job.daemon(), bool)
  elif not job.has_daemon() and job.has_service():
    return fully_interpolated(job.service(), bool)
  else:
    raise InvalidConfig('Specified both daemon and service bits!')


# TODO(wickman) Due to MESOS-2718 we should revert to using the MesosTaskInstance.
#
# Using the MesosJob instead of the MesosTaskInstance was to allow for
# planned future use of fields such as 'cluster' and to allow for conversion
# from Job=>Task to be done entirely on the executor, but instead this had
# made it impossible to run idempotent updates.
#
# In the meantime, we are erasing fields of the Job that are controversial.
# This achieves roughly the same effect as using the MesosTaskInstance.
# The future work is tracked at MESOS-2727.
ALIASED_FIELDS = (
  'cron_policy',
  'cron_collision_policy',
  'update_config',
  'daemon',
  'service',
  'instances'
)


def filter_aliased_fields(job):
  return job(**dict((key, Empty) for key in ALIASED_FIELDS))


def assert_valid_field(field, identifier):
  VALID_IDENTIFIER = re.compile(GOOD_IDENTIFIER_PATTERN_PYTHON)
  if not isinstance(identifier, Compatibility.string):
    raise InvalidConfig("%s must be a string" % field)
  if not VALID_IDENTIFIER.match(identifier):
    raise InvalidConfig("Invalid %s '%s'" % (field, identifier))
  return identifier


MESOS_INSTANCE_REF = Ref.from_address('mesos.instance')
THERMOS_PORT_SCOPE_REF = Ref.from_address('thermos.ports')
THERMOS_TASK_ID_REF = Ref.from_address('thermos.task_id')


# TODO(wickman) Make this a method directly on an AuroraConfig so that we don't
# need the packages/ports shenanigans.
def convert(job, packages=frozenset(), ports=frozenset()):
  """Convert a Pystachio MesosJob to an Aurora Thrift JobConfiguration."""

  owner = Identity(role=fully_interpolated(job.role()), user=getpass.getuser())
  key = JobKey(
    role=assert_valid_field('role', fully_interpolated(job.role())),
    environment=assert_valid_field('environment', fully_interpolated(job.environment())),
    name=assert_valid_field('name', fully_interpolated(job.name())))

  task_raw = job.task()

  MB = 1024 * 1024
  task = TaskConfig()

  def not_empty_or(item, default):
    return default if item is Empty else fully_interpolated(item)

  # job components
  task.jobName = fully_interpolated(job.name())
  task.environment = fully_interpolated(job.environment())
  task.production = fully_interpolated(job.production(), bool)
  task.isService = select_service_bit(job)
  task.maxTaskFailures = fully_interpolated(job.max_task_failures())
  task.priority = fully_interpolated(job.priority())
  task.contactEmail = not_empty_or(job.contact(), None)

  # Add package tuples to a task, to display in the scheduler UI.
  task.packages = frozenset(
      Package(role=str(role), name=str(package_name), version=int(version))
      for role, package_name, version in packages)

  # task components
  if not task_raw.has_resources():
    raise InvalidConfig('Task must specify resources!')

  if (fully_interpolated(task_raw.resources().ram()) == 0
      or fully_interpolated(task_raw.resources().disk()) == 0):
    raise InvalidConfig('Must specify ram and disk resources, got ram:%r disk:%r' % (
      fully_interpolated(task_raw.resources().ram()),
      fully_interpolated(task_raw.resources().disk())))

  task.numCpus = fully_interpolated(task_raw.resources().cpu())
  task.ramMb = fully_interpolated(task_raw.resources().ram()) / MB
  task.diskMb = fully_interpolated(task_raw.resources().disk()) / MB
  if task.numCpus <= 0 or task.ramMb <= 0 or task.diskMb <= 0:
    raise InvalidConfig('Task has invalid resources.  cpu/ramMb/diskMb must all be positive: '
        'cpu:%r ramMb:%r diskMb:%r' % (task.numCpus, task.ramMb, task.diskMb))

  task.owner = owner
  task.requestedPorts = ports
  task.taskLinks = not_empty_or(job.task_links(), {})
  task.constraints = constraints_to_thrift(not_empty_or(job.constraints(), {}))

  underlying, refs = job.interpolate()

  # need to fake an instance id for the sake of schema checking
  underlying_checked = underlying.bind(mesos = {'instance': 31337})
  try:
    ThermosTaskValidator.assert_valid_task(underlying_checked.task())
  except ThermosTaskValidator.InvalidTaskError as e:
    raise InvalidConfig('Task is invalid: %s' % e)
  if not underlying_checked.check().ok():
    raise InvalidConfig('Job not fully specified: %s' % underlying.check().message())

  unbound = []
  for ref in refs:
    if ref == THERMOS_TASK_ID_REF or ref == MESOS_INSTANCE_REF or (
        Ref.subscope(THERMOS_PORT_SCOPE_REF, ref)):
      continue
    unbound.append(ref)

  if unbound:
    raise InvalidConfig('Config contains unbound variables: %s' % ' '.join(map(str, unbound)))

  cron_schedule = not_empty_or(job.cron_schedule(), '')
  cron_policy = select_cron_policy(job.cron_policy(), job.cron_collision_policy())

  task.executorConfig = ExecutorConfig(
      name=AURORA_EXECUTOR_NAME,
      data=filter_aliased_fields(underlying).json_dumps())

  return JobConfiguration(
      key=key,
      owner=owner,
      cronSchedule=cron_schedule,
      cronCollisionPolicy=cron_policy,
      taskConfig=task,
      instanceCount=fully_interpolated(job.instances()))
