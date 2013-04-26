import textwrap

from twitter.common.lang import Compatibility
from twitter.thermos.config.loader import ConfigLoader

from .schema import Job


class AuroraConfigLoader(ConfigLoader):
  SCHEMA = textwrap.dedent("""
    from pystachio import *
    from twitter.mesos.config.schema import *
  """)

  @classmethod
  def load(cls, loadable):
    environment = cls.schema()
    ConfigLoader.load(loadable, environment)
    return environment

  @classmethod
  def load_json(cls, filename):
    with open(filename) as fp:
      return Job.json_load(fp)
