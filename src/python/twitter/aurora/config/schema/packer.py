from textwrap import dedent

from twitter.thermos.config.schema import Process

from pystachio import List, String, Struct

__all__ = (
  'Packer',
  'PackerObject',
)


# The object bound into the {{packer}} namespace.
# Referenced by
#  {{packer[role][name][version]}}
#
# Where version =
#    number (integer)
#    'live' (live package)
#    'latest' (highest version number)
#
# For example if you'd like to create a copy process for a particular
# package,
#   copy_latest = Process(
#     name = 'copy-{{package_name}}',
#     cmdline = '{{packer[{{role}}][{{package_name}}][latest].copy_command}}')
#   processes = [
#     copy_latest.bind(package_name = 'labrat'),
#     copy_latest.bind(package_name = 'packer')
#   ]
class PackerObject(Struct):
  package = String
  package_uri = String
  copy_command = String


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
     elif [[ "{{{{pkg}}.package}}" == *".shar" ]]; then
       sh {{{{pkg}}.package}} && _delete_pkg
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
