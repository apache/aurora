import json
import os

from twitter.common.lang import Compatibility
from .schema import Job


SCHEMA_PREAMBLE = """
from pystachio import *
from twitter.mesos.config.schema import *
"""


def deposit_schema(environment):
  Compatibility.exec_function(
    compile(SCHEMA_PREAMBLE, "<exec_function>", "exec"), environment)


class AuroraConfigLoader(object):
  SCHEMA = {}
  deposit_schema(SCHEMA)
  
  class Error(Exception): pass
  class InvalidConfigError(Error): pass

  @classmethod
  def load_into(cls, filename):
    environment = {}
    deposit_stack = [os.path.dirname(filename)]
    def ast_executor(config_file, env):
      actual_file = os.path.join(deposit_stack[-1], config_file)
      deposit_stack.append(os.path.dirname(actual_file))
      with open(actual_file) as fp:
        Compatibility.exec_function(compile(fp.read(), actual_file, 'exec'), env)
      deposit_stack.pop()
    def export(*args, **kw):
      pass
    environment.update(cls.SCHEMA)
    environment.update(
      include=lambda fn: ast_executor(fn, environment),
      export=export)
    try:
      ast_executor(os.path.basename(filename), environment)
    except SyntaxError as e:
      raise cls.InvalidConfigError(str(e))
    return environment

  @classmethod
  def load(cls, filename):
    return cls.load_into(filename)

  @staticmethod
  def load_json(filename):
    with open(filename) as fp:
      js = json.load(fp)
    return Job(js)
