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
  def load_from_filename(cls, filename):
    deposit_stack = [os.path.dirname(filename)]
    entry_point = os.path.basename(filename)
    environment = {}
    def ast_filename_executor(config_filename):
      actual_filename = os.path.join(deposit_stack[-1], config_filename)
      deposit_stack.append(os.path.dirname(actual_filename))
      with open(actual_filename) as fp:
        Compatibility.exec_function(compile(fp.read(), actual_filename, 'exec'), environment)
      deposit_stack.pop()
    environment.update(cls.SCHEMA)
    environment.update(
      include=ast_filename_executor,
      export=lambda *args, **kws: None)
    try:
      ast_filename_executor(entry_point)
    except SyntaxError as e:
      raise cls.InvalidConfigError(str(e))
    return environment

  @classmethod
  def load_from_fp(cls, fp):
    environment = {}
    def ast_fp_executor(fp):
      Compatibility.exec_function(compile(fp.read(), '<resource: %s>' % fp, 'exec'), environment)
    def include(*args, **kws):
      raise cls.InvalidConfigError('include is not supported when loading from fp')
    environment.update(cls.SCHEMA)
    environment.update(
      include=include,
      export=lambda *args, **kws: None)
    try:
      ast_fp_executor(fp)
    except SyntaxError as e:
      raise cls.InvalidConfigError(str(e))
    return environment

  @classmethod
  def load_from(cls, filename_or_fp):
    if isinstance(filename_or_fp, Compatibility.string):
      return cls.load_from_filename(filename_or_fp)
    else:
      return cls.load_from_fp(filename_or_fp)

  @classmethod
  def load(cls, filename_or_fp):
    return cls.load_from(filename_or_fp)

  @staticmethod
  def load_json(filename):
    with open(filename) as fp:
      js = json.load(fp)
    return Job(js)
