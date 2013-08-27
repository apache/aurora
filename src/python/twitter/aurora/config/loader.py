import json
import textwrap

from .schema import Job

from pystachio.config import Config as PystachioConfig


class AuroraConfigLoader(PystachioConfig):
  DEFAULT_SCHEMA = textwrap.dedent("""
    from pystachio import *
    from twitter.aurora.config.schema import *
  """)

  @classmethod
  def load(cls, loadable):
    return cls.load_raw(loadable).environment

  @classmethod
  def load_raw(cls, loadable):
    return cls(loadable)

  @classmethod
  def load_json(cls, filename):
    with open(filename) as fp:
      return Job.json_load(fp)

  @classmethod
  def loads_json(cls, string):
    return Job(json.loads(string))
