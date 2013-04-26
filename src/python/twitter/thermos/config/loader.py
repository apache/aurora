import copy
import json
import os
import re
import textwrap

from pystachio import Ref
from twitter.common.dirutil import safe_open
from twitter.common.lang import Compatibility
from twitter.thermos.base.planner import TaskPlanner
from twitter.thermos.config.schema import Task

import pkg_resources


class PortExtractor(object):
  class InvalidPorts(Exception): pass

  @staticmethod
  def extract(obj):
    port_scope = Ref.from_address('thermos.ports')
    _, uninterp = obj.interpolate()
    ports = []
    for ref in uninterp:
      subscope = port_scope.scoped_to(ref)
      if subscope is not None:
        if not subscope.is_index():
          raise PortExtractor.InvalidPorts(
            'Bad port specification "%s" (should be of form "thermos.ports[name]"' % ref.address())
        ports.append(subscope.action().value)
    return ports


class ThermosProcessWrapper(object):
  # >=1 characters && anything but NULL and '/'
  VALID_PROCESS_NAME_RE = re.compile(r'^[^./][^/]*$')
  class InvalidProcess(Exception): pass

  def __init__(self, process):
    self._process = process

  def ports(self):
    try:
      return PortExtractor.extract(self._process)
    except PortExtractor.InvalidPorts:
      raise self.InvalidProcess('Process has invalid ports scoping!')

  @staticmethod
  def assert_valid_process_name(name):
    if not ThermosProcessWrapper.VALID_PROCESS_NAME_RE.match(name):
      raise ThermosProcessWrapper.InvalidProcess('Invalid process name: %s' % name)


class ThermosTaskWrapper(object):
  class InvalidTask(Exception): pass

  def __init__(self, task, bindings=None, strict=True):
    if bindings:
      task = task.bind(*bindings)
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
        try:
          ports.update(ThermosProcessWrapper(process).ports())
        except ThermosProcessWrapper.InvalidProcess:
          raise self.InvalidTask('Task has invalid process: %s' % process)
    return ports

  def to_json(self):
    return json.dumps(self._task.get())

  def to_file(self, filename):
    ti, _ = self._task.interpolate()
    with safe_open(filename, 'w') as fp:
      json.dump(ti.get(), fp)

  @staticmethod
  def from_file(filename, **kw):
    try:
      with safe_open(filename) as fp:
        task = Task.json_load(fp)
      return ThermosTaskWrapper(task, **kw)
    except Exception as e:
      return None


# TODO(wickman) These should be validators pushed onto ThermosConfigLoader.plugins
class ThermosTaskValidator(object):
  class InvalidTaskError(Exception): pass

  @classmethod
  def assert_valid_task(cls, task):
    cls.assert_valid_names(task)
    cls.assert_typecheck(task)
    cls.assert_valid_plan(task)

  @classmethod
  def assert_valid_plan(cls, task):
    try:
      TaskPlanner(task, process_filter=lambda proc: proc.final().get() == False)
      TaskPlanner(task, process_filter=lambda proc: proc.final().get() == True)
    except TaskPlanner.InvalidSchedule as e:
      raise cls.InvalidTaskError('Task has invalid plan: %s' % e)

  @classmethod
  def assert_valid_names(cls, task):
    for process in task.processes():
      name = process.name().get()
      try:
        ThermosProcessWrapper.assert_valid_process_name(name)
      except ThermosProcessWrapper.InvalidProcess as e:
        raise cls.InvalidTaskError('Task has invalid process: %s' % e)

  @classmethod
  def assert_typecheck(cls, task):
    typecheck = task.check()
    if not typecheck.ok():
      raise cls.InvalidTaskError('Failed to fully evaluate task: %s' %
        typecheck.message())

  @classmethod
  def assert_valid_ports(cls, task, portmap):
    for port in ThermosTaskWrapper(task).ports():
      if port not in portmap:
        raise cls.InvalidTaskError('Task requires unbound port %s!' % port)

  @classmethod
  def assert_same_task(cls, spec, task):
    active_task = spec.given(state='active').getpath('task_path')
    if os.path.exists(active_task):
      task_on_disk = ThermosTaskWrapper.from_file(active_task)
      if not task_on_disk or task_on_disk.task != task:
        raise cls.InvalidTaskError('Task differs from on disk copy: %r vs %r' % (
            task_on_disk.task if task_on_disk else None, task))


# TODO(wickman) Push this down into pystachio as part of
# https://github.com/wickman/pystachio/issues/6 and
# https://github.com/wickman/pystachio/issues/7
class ConfigLoader(object):
  class Error(Exception): pass
  class InvalidConfigError(Error): pass
  class NotFound(Error): pass

  SCHEMA = ""
  _SCHEMA_ENVIRONMENT = {}

  @classmethod
  def schema(cls):
    if not cls._SCHEMA_ENVIRONMENT:
      Compatibility.exec_function(
          compile(cls.SCHEMA, "<exec_function>", "exec"), cls._SCHEMA_ENVIRONMENT)
    return copy.copy(cls._SCHEMA_ENVIRONMENT)

  @classmethod
  def compile_into(cls, data, data_name, environment):
    try:
      Compatibility.exec_function(compile(data, data_name, 'exec'), environment)
    except SyntaxError as e:
      raise cls.InvalidConfigError(str(e))

  @classmethod
  def fp_executor(cls):
    def ast_executor(fp, env):
      if hasattr(fp, 'read') and callable(fp.read):
        cls.compile_into(fp.read(), '<resource: %s>' % fp, env)
      else:
        raise cls.InvalidConfigError('Configurations loaded from file cannot include other files.')
    return ast_executor

  @classmethod
  def file_executor(cls, filename):
    deposit_stack = [os.path.dirname(filename)]
    def ast_executor(config_file, env):
      actual_file = os.path.join(deposit_stack[-1], config_file)
      deposit_stack.append(os.path.dirname(actual_file))
      with open(actual_file) as fp:
        cls.compile_into(fp.read(), actual_file, env)
      deposit_stack.pop()
    return ast_executor

  @classmethod
  def resource_exists(cls, filename):
    module_base, module_file = os.path.split(filename)
    module_base = module_base.replace(os.sep, '.')
    try:
      return pkg_resources.resource_exists(module_base, module_file) and (
          not pkg_resources.resource_isdir(module_base, module_file))
    except (ValueError, ImportError):
      return False

  @classmethod
  def module_executor(cls, filename):
    deposit_stack = [os.path.dirname(filename)]
    def ast_executor(config_file, env):
      actual_file = os.path.join(deposit_stack[-1], config_file)
      deposit_stack.append(os.path.dirname(actual_file))
      module_base, module_file = os.path.split(actual_file)
      module_base = module_base.replace(os.sep, '.')
      cls.compile_into(pkg_resources.resource_string(module_base, module_file), actual_file, env)
      deposit_stack.pop()
    return ast_executor

  @classmethod
  def choose_executor_and_base(cls, obj):
    if isinstance(obj, Compatibility.string):
      if os.path.isfile(obj):
        return cls.file_executor(obj), os.path.basename(obj)
      elif cls.resource_exists(obj):
        return cls.module_executor(obj), os.path.basename(obj)
    elif hasattr(obj, 'read') and callable(obj.read):
      return cls.fp_executor(), obj
    raise cls.NotFound('Could not load resource %s' % obj)

  @classmethod
  def load(cls, loadable, environment=None):
    """
      Load a loadable object.  A loadable object may be one of:
        - a file-like object
        - a file path
        - a zipimported file path
    """
    environment = environment or {}
    ast_executor, loadable_base = cls.choose_executor_and_base(loadable)
    environment.update(include=lambda fn: ast_executor(fn, environment))
    ast_executor(loadable_base, environment)
    return environment


class ThermosConfigLoader(ConfigLoader):
  SCHEMA = textwrap.dedent("""
    from pystachio import *
    from twitter.thermos.config.schema import *
  """)

  @classmethod
  def load(cls, loadable, **kw):
    tc = cls()
    def export(task):
      if isinstance(task, dict):
        task = Task(task)
      tc.add_task(ThermosTaskWrapper(task, **kw))
    environment = cls.schema()
    environment.update(export=export)
    ConfigLoader.load(loadable, environment)
    return tc

  @classmethod
  def load_json(cls, filename, **kw):
    tc = cls()
    task = ThermosTaskWrapper.from_file(filename, **kw)
    if task:
      ThermosTaskValidator.assert_valid_task(task.task())
      tc.add_task(task)
    return tc

  def __init__(self):
    self._exported_tasks = []

  def add_task(self, task):
    self._exported_tasks.append(task)

  def tasks(self):
    return self._exported_tasks
