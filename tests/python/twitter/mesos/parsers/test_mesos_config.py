from __future__ import print_function

from contextlib import contextmanager
from copy import deepcopy

import pytest

from twitter.common.contextutil import temporary_file
from twitter.mesos.parsers.mesos_config import MesosConfig
from twitter.mesos.parsers.mesos_thrift import convert as mesos_to_thrift

from gen.twitter.mesos.ttypes import (
  Constraint,
  LimitConstraint,
  TaskConstraint)


HELLO_WORLD = {
  'name': 'hello_world',
  'role': 'john_doe',
  'cluster': 'smf1-test',
  'task': {
    'start_command': 'echo "hello world"',
    'num_cpus': 0.1,
    'ram_mb': 64,
    'disk_mb': 64
  }
}


@contextmanager
def disk_config(job):
  with temporary_file() as fp:
    print('HELLO_WORLD = %r\njobs = [HELLO_WORLD]' % job, file=fp)
    fp.flush()
    yield fp.name


def test_simple_config():
  with disk_config(HELLO_WORLD) as filename:
    MesosConfig(filename)


def test_missing_fields():
  for field in ('name', 'role', 'cluster', 'task'):
    broken = deepcopy(HELLO_WORLD)
    broken.pop(field)
    with disk_config(broken) as filename:
      with pytest.raises(MesosConfig.InvalidConfig):
        MesosConfig(filename)

  for task_field in ('start_command', 'num_cpus', 'ram_mb', 'disk_mb'):
    broken = deepcopy(HELLO_WORLD)
    broken['task'].pop(task_field)
    with disk_config(broken) as filename:
      with pytest.raises(MesosConfig.InvalidConfig):
        MesosConfig(filename)


def test_zeroed_fields():
  for task_field in ('num_cpus', 'ram_mb', 'disk_mb'):
    broken = deepcopy(HELLO_WORLD)
    broken['task'][task_field] = 0
    with disk_config(broken) as filename:
      with pytest.raises(MesosConfig.InvalidConfig):
        MesosConfig(filename)


def test_constraints():
  hw = deepcopy(HELLO_WORLD)
  hw['constraints'] = { 'host': 'limit:1' }
  with disk_config(hw) as filename:
    config = MesosConfig(filename)

  for tti in config.job().taskConfigs:
    assert len(tti.constraints) == 1
    const = list(tti.constraints)[0]
    assert const == Constraint(
       name = 'host', constraint = TaskConstraint(limit = LimitConstraint(limit = 1)))
