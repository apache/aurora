from textwrap import dedent

from .schema import Process

__all__ = (
  'Packer',
)


class Packer(object):
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
      __package_version = version
    )

  @classmethod
  def install(cls, name, role='{{role}}', version='live'):
    return cls.copy(name, role, version, unpack=True)
