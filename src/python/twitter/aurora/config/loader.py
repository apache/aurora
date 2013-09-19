import json
import pkgutil
import textwrap

from twitter.aurora.config.schema import base as base_schema

from pystachio.config import Config as PystachioConfig


class AuroraConfigLoader(PystachioConfig):
  # TODO(wickman) Kill this once we've standardized on the src/aurora tree
  # or an aurora configuration repository.
  SCHEMA_ROOT = 'twitter.aurora.config.schema'
  SCHEMA_MODULES = [base_schema]

  @classmethod
  def assembled_schema(cls, schema_modules):
    default_schema = [super(AuroraConfigLoader, cls).DEFAULT_SCHEMA]
    default_schema.extend('from %s import *' % module.__name__ for module in schema_modules)
    return '\n'.join(default_schema)

  @classmethod
  def register_schema(cls, schema_module):
    cls.SCHEMA_MODULES.append(schema_module)
    cls.DEFAULT_SCHEMA = cls.assembled_schema(cls.SCHEMA_MODULES)

  @classmethod
  def register_all_schemas(cls):
    module_root = __import__(cls.SCHEMA_ROOT, fromlist=[cls.SCHEMA_ROOT])
    for _, submodule, is_package in pkgutil.iter_modules(module_root.__path__):
      if is_package:
        continue
      cls.register_schema(__import__('%s.%s' % (cls.SCHEMA_ROOT, submodule),
          fromlist=[cls.SCHEMA_ROOT]))

  @classmethod
  def load(cls, loadable):
    return cls.load_raw(loadable).environment

  @classmethod
  def load_raw(cls, loadable):
    return cls(loadable)

  @classmethod
  def load_json(cls, filename):
    with open(filename) as fp:
      return base_schema.Job.json_load(fp)

  @classmethod
  def loads_json(cls, string):
    return base_schema.Job(json.loads(string))


AuroraConfigLoader.register_all_schemas()
