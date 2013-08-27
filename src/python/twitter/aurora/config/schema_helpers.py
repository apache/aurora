from textwrap import dedent

from .schema_base import Process

from pystachio import List, String, Struct

__all__ = (
  'Build',
  'BuildSpec',
  'Jenkins',
  'Packer',
)


class Packer(object):
  """
    Packer Helper provides functions to produce idiomatic thermos job descriptions:
    1. Make use of Packer.copy or Packer.install
    2. Avoid reliance on internal details like copy_command
  """
  COPY_COMMAND = "{{{{pkg}}.copy_command}}"

  UNPACK_COMMAND = COPY_COMMAND + dedent("""

     function _delete_pkg() { rm -f {{{{pkg}}.package}}; }

     if [[ "{{{{pkg}}.package}}" == *".tar" ]]; then
       tar -xf {{{{pkg}}.package}} && _delete_pkg
     elif [[ "{{{{pkg}}.package}}" == *".tar.gz" || "{{{{pkg}}.package}}" == *".tar.gz" ]]; then
       tar -xzf {{{{pkg}}.package}} && _delete_pkg
     elif [[ "{{{{pkg}}.package}}" == *".tar.bz2" || "{{{{pkg}}.package}}" == *".tbz2" ]]; then
       (bzip2 -cd {{{{pkg}}.package}} | tar -xf -) && _delete_pkg
     elif [[ "{{{{pkg}}.package}}" == *".zip" ]]; then
       unzip {{{{pkg}}.package}} && _delete_pkg
     fi
  """)

  PROCESS = Process(
      name = 'stage_{{__package_name}}',
  ).bind(pkg = 'packer[{{__package_role}}][{{__package_name}}][{{__package_version}}]')

  @classmethod
  def copy(cls, name, role='{{role}}', version='live', unpack=False):
    return cls.PROCESS(
      cmdline = cls.UNPACK_COMMAND if unpack else cls.COPY_COMMAND
    ).bind(
      __package_name = name,
      __package_role = role,
      __package_version = version,
    )

  @classmethod
  def install(cls, name, role='{{role}}', version='live'):
    return cls.copy(name, role, version, unpack=True)


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
