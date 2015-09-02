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

import json

from thrift.Thrift import TException
from thrift.TSerialization import deserialize as thrift_deserialize
from twitter.common import log

from apache.aurora.config.port_resolver import PortResolver
from apache.aurora.config.schema.base import MesosJob, MesosTaskInstance
from apache.aurora.config.thrift import task_instance_from_job
from apache.thermos.config.loader import ThermosTaskValidator

from gen.apache.aurora.api.ttypes import AssignedTask


class TaskInfoError(ValueError):
  pass


class UnexpectedUnboundRefsError(TaskInfoError):
  pass


def assigned_task_from_mesos_task(task):
  """Deserialize AssignedTask from a launchTask task protocol buffer."""
  try:
    assigned_task = thrift_deserialize(AssignedTask(), task.data)
  except (EOFError, TException) as e:
    raise TaskInfoError('Could not deserialize task! %s' % e)
  return assigned_task


def mesos_job_from_assigned_task(assigned_task):
  """Deserialize a MesosJob pystachio struct from an AssignedTask."""
  thermos_task = assigned_task.task.executorConfig.data
  try:
    json_blob = json.loads(thermos_task)
  except (TypeError, ValueError):
    return None
  if 'instance' in json_blob:
    # This is a MesosTaskInstance so we cannot get a MesosJob from this assigned_task
    return None
  return MesosJob.json_loads(thermos_task)


def mesos_task_instance_from_assigned_task(assigned_task):
  """Deserialize MesosTaskInstance from an AssignedTask thrift."""
  thermos_task = assigned_task.task.executorConfig.data

  if not thermos_task:
    raise TaskInfoError('Task did not have a thermos config!')

  try:
    json_blob = json.loads(thermos_task)
  except (TypeError, ValueError) as e:
    raise TaskInfoError('Could not deserialize thermos config: %s' % e)

  # TODO(wickman) Determine if there are any serialized MesosTaskInstances in the wild;
  # kill this code if not.
  if 'instance' in json_blob:
    return MesosTaskInstance.json_loads(thermos_task)

  # This is a MesosJob
  task_instance = task_instance_from_job(
      MesosJob.json_loads(thermos_task),
      assigned_task.instanceId,
      assigned_task.slaveHost)

  try:
    ThermosTaskValidator.assert_valid_task(task_instance.task())
    ThermosTaskValidator.assert_all_refs_bound(task_instance)
  except ThermosTaskValidator.InvalidTaskError as e:
    raise UnexpectedUnboundRefsError('Got invalid task: %s' % e)

  task_instance, _ = task_instance.interpolate()
  return task_instance


def resolve_ports(mesos_task, portmap):
  """Given a MesosTaskInstance and the portmap of resolved ports from the scheduler,
     create a fully resolved map of port name => port number for the thermos
     runner and discovery manager."""
  task_portmap = mesos_task.announce().portmap().get() if mesos_task.has_announce() else {}
  task_portmap.update(portmap)
  task_portmap = PortResolver.resolve(task_portmap)

  for name, port in task_portmap.items():
    if not isinstance(port, int):
      log.warning('Task has unmapped port: %s => %s' % (name, port))

  return dict((name, port) for (name, port) in task_portmap.items() if isinstance(port, int))
