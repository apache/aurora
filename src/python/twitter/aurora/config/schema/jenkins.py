from twitter.thermos.config.schema import Process

from .packer import Packer

__all__ = (
  'Jenkins',
)


class Jenkins(Packer):
  """
    Jenkins Helper provides functions to produce idiomatic thermos job descriptions:
    1. Make use of Jenkins.copy or Jenkins.install
    2. Avoid reliance on internal details like copy_command
  """
  PROCESS = Process(
      name = 'jenkins-artifact-copy-{{__jenkins_project}}',
  ).bind(pkg = 'jenkins[{{__jenkins_project}}][{{__jenkins_build_number}}]')

  @classmethod
  def copy(cls, project, build_number='latest', unpack=False):
    return cls.PROCESS(
      cmdline = cls.UNPACK_COMMAND if unpack else cls.COPY_COMMAND
    ).bind(
      __jenkins_project = project,
      __jenkins_build_number = build_number,
    )

  @classmethod
  def install(cls, project, build_number='latest'):
    return cls.copy(project, build_number, unpack=True)
