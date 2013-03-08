import copy
import getpass
import json

from twitter.common.lang import Compatibility
from twitter.thermos.config.loader import (
  ThermosTaskValidator,
  ThermosTaskWrapper
)

from gen.twitter.mesos.ttypes import (
  Constraint,
  LimitConstraint,
  TaskConstraint,
  ValueConstraint,
  CronCollisionPolicy,
  Identity,
  JobConfiguration,
  Package,
  TwitterTaskInfo,
)

from .schema import (
  MesosContext,
  MesosTaskInstance
)

from pystachio import Empty

__all__ = (
  'InvalidConfig',
  'convert'
)


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
  ti = MesosTaskInstance(task=job.task(),
                         layout=job.layout(),
                         role=job.role(),
                         health_check_interval_secs=job.health_check_interval_secs(),
                         instance=instance)
  if job.has_announce():
    ti = ti(announce=job.announce())
  if job.has_environment():
    ti = ti(environment=job.environment())
  return ti.bind(mesos=instance_context).interpolate()


def translate_cron_policy(policy):
  cron_policy = CronCollisionPolicy._NAMES_TO_VALUES.get(policy.get())
  if cron_policy is None:
    raise ValueError('Invalid cron policy: %s' % policy.get())
  return cron_policy


def select_cron_policy(cron_policy, cron_collision_policy):
  if cron_policy is Empty and cron_collision_policy is Empty:
    return CronCollisionPolicy.KILL_EXISTING
  elif cron_policy is not Empty and cron_collision_policy is Empty:
    return translate_cron_policy(cron_policy)
  elif cron_policy is Empty and cron_collision_policy is not Empty:
    return translate_cron_policy(cron_collision_policy)
  else:
    raise ValueError('Specified both cron_policy and cron_collision_policy!')


def convert(job, packages=[]):
  if not job.role().check().ok():
    raise ValueError(job.role().check().message())

  owner = Identity(role=str(job.role()), user=getpass.getuser())

  task_raw = job.task()

  MB = 1024 * 1024
  task = TwitterTaskInfo()

  def not_empty_or(item, default):
    return default if item is Empty else item.get()

  # job components
  task.jobName = job.name().get()
  task.production = bool(job.production().get())
  task.isDaemon = bool(job.daemon().get())
  task.maxTaskFailures = job.max_task_failures().get()
  task.priority = job.priority().get()
  if job.has_health_check_interval_secs():
    task.healthCheckIntervalSecs = job.health_check_interval_secs().get()
  task.contactEmail = not_empty_or(job.contact(), None)

  # Add package tuples (role, package_name, version) to a task, to display in the scheduler UI.
  task.packages = [Package(str(p[0]), str(p[1]), int(p[2])) for p in packages] if packages else None

  # task components
  if not task_raw.has_resources():
    raise ValueError('Task must specify resources!')

  if task_raw.resources().ram().get() == 0 or task_raw.resources().disk().get() == 0:
    raise ValueError('Must specify ram and disk resources, got ram:%r disk:%r' % (
      task_raw.resources().ram().get(), task_raw.resources().disk().get()))

  task.numCpus = task_raw.resources().cpu().get()
  task.ramMb = task_raw.resources().ram().get() / MB
  task.diskMb = task_raw.resources().disk().get() / MB
  if task.numCpus <= 0 or task.ramMb <= 0 or task.diskMb <= 0:
    raise ValueError('Task has invalid resources.  cpu/ramMb/diskMb must all be positive: '
        'cpu:%r ramMb:%r diskMb:%r' % (task.numCpus, task.ramMb, task.diskMb))

  task.owner = owner
  task.requestedPorts = ThermosTaskWrapper(task_raw, strict=False).ports()
  task.taskLinks = not_empty_or(job.task_links(), {})
  task.constraints = constraints_to_thrift(not_empty_or(job.constraints(), {}))

  underlying, _ = job.interpolate()
  # need to fake an instance id for the sake of schema checking
  underlying_checked = underlying.bind(mesos = {'instance': 31337})
  try:
    ThermosTaskValidator.assert_valid_task(underlying_checked.task())
  except ThermosTaskValidator.InvalidTaskError as e:
    raise ValueError('Task is invalid: %s' % e)
  if not underlying_checked.check().ok():
    raise ValueError('Job not fully specified: %s' % underlying.check().message())
  task.thermosConfig = underlying.json_dumps()

  cron_schedule = job.cron_schedule().get() if job.has_cron_schedule() else ''
  cron_policy = select_cron_policy(job.cron_policy(), job.cron_collision_policy())

  return JobConfiguration(
    name=str(job.name()),
    owner=owner,
    cronSchedule=cron_schedule,
    cronCollisionPolicy=cron_policy,
    taskConfig=task,
    shardCount=job.instances().get())
