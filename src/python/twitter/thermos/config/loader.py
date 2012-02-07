import copy
import json
import os
import sys

from pystachio import Ref
from twitter.common.dirutil import safe_open
from twitter.common.lang.compatibility import *
from twitter.thermos.config.schema import Task

SCHEMA_PREAMBLE = """
from pystachio import *
from twitter.thermos.config.schema import *
from twitter.thermos.config.dsl import *
"""

def deposit_schema(environment):
  exec_function(compile(SCHEMA_PREAMBLE, "<exec_function>", "exec"), environment)


class ThermosProcessWrapper(object):
  class InvalidProcess(Exception): pass

  def __init__(self, process):
    self._process = process

  def ports(self):
    port_scope = Ref.from_address('thermos.ports')
    _, uninterp = self._process.interpolate()
    ports = []
    for ref in uninterp:
      subscope = port_scope.scoped_to(ref)
      if subscope is not None:
        assert subscope.is_index()
        ports.append(subscope.action().value)
    return ports


class ThermosTaskWrapper(object):
  class InvalidTask(Exception): pass

  def __init__(self, task, strict=True):
    if not task.check().ok() and strict:
      raise ThermosTaskWrapper.InvalidTask(task.check().message())
    self._task = task

  @property
  def task(self):
    return self._task

  def ports(self):
    ti, _ = self._task.interpolate()
    ports = set()
    if ti.has_processes():
      for process in ti.processes():
        ports.update(ThermosProcessWrapper(process).ports())
    return ports

  def to_json(self):
    return json.dumps(self._task.get())

  def to_file(self, filename):
    ti, _ = self._task.interpolate()
    with safe_open(filename, 'w') as fp:
      json.dump(ti.get(), fp)

  @staticmethod
  def from_file(filename):
    try:
      with safe_open(filename) as fp:
        js = json.load(fp)
      return ThermosTaskWrapper(Task(js))
    except Exception as e:
      return None


class ThermosConfigLoader(object):
  SCHEMA = {}
  deposit_schema(SCHEMA)

  @staticmethod
  def load(filename):
    tc = ThermosConfigLoader()
    def export(task):
      tc.add_task(ThermosTaskWrapper(task))
    schema_copy = copy.copy(ThermosConfigLoader.SCHEMA)
    schema_copy['export'] = export
    with open(filename) as fp:
      exec_function(compile(fp.read(), filename, 'exec'), schema_copy)
    return tc

  @staticmethod
  def load_json(filename):
    tc = ThermosConfigLoader()
    tc.add_task(ThermosTaskWrapper.from_file(filename))
    return tc

  def __init__(self):
    self._exported_tasks = []

  def add_task(self, task):
    self._exported_tasks.append(task)

  def tasks(self):
    return self._exported_tasks
