from twitter.thermos.config.schema import Process

from .packer import Packer

from pystachio import List, String, Struct

__all__ = (
  'Build',
  'BuildSpec',
)


class BuildSpec(Struct):
  """This build spec is available for jobs to expand build[build_profile]"""
  test_command = String
  build_command = String
  gitsha = String
  artifact_globs = List(String)


class Build(Packer):
  """Build Helper provides functions to produce idiomatic thermos job descriptions:
     1. Make use of Build.copy or Build.install
     2. Avoid reliance on internal details like copy_command

     build[build_spec_name].copy_command first builds the artifact based on the spec
     if the artifact has not been previously uploaded
  """

  PROCESS = Process(
      name = 'build-artifact-copy-{{__build_spec_name}}',
  ).bind(pkg = 'build[{{__build_spec_name}}]')

  @classmethod
  def copy(cls, name, unpack=False):
    return cls.PROCESS(
      cmdline = Packer.UNPACK_COMMAND if unpack else Packer.COPY_COMMAND
    ).bind(
      __build_spec_name = name,
    )

  @classmethod
  def install(cls, name):
    return cls.copy(name, unpack=True)
