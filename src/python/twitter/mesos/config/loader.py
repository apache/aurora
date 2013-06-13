import textwrap

from twitter.common.lang import Compatibility

from .schema import Job

from pystachio.config import Config


class AuroraConfigLoader(Config):
  DEFAULT_SCHEMA = textwrap.dedent("""
    from pystachio import *
    from twitter.mesos.config.schema import *
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
