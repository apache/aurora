try:
  from pipes import quote
except ImportError:
  from shlex import quote

from textwrap import dedent

from twitter.aurora.config.schema.base import Process


def HealthCheckedProcess(**kw):
  """A Thermos Process that is wrapped by a health-checking proxy.

     Takes the same keyword arguments as a regular Process.  The following
     additional keyword arguments are accepted:

       application_health_port [required]:
         The health port that the application actually exports
         (the proxy health checker will listen on {{thermos.ports[health]}})

       teardown_command [optional]:
         The teardown command that is invoked when the underlying process fails its health checks.
         The teardown command is always passed as its first parameter the pid of the underlying
         forked process.

       health_check_config [optional]:
         If specified, take a HealthCheckConfig for the proxy health checker, otherwise it will
         inherit the health_check_config from the Job.
  """
  health_check_package = 'packer[mesos][__health_checker_proxy][live]'
  additional_bindings = {'health_check_package': health_check_package}
  additional_arguments = []

  if 'cmdline' not in kw:
    raise ValueError('HealthCheckProcess requires a command.')

  if 'application_health_port' not in kw:
    raise ValueError('HealthCheckedProcess requires application_health_port to be specified.')

  additional_arguments.append(
      '--check_port={{thermos.ports[%s]}}' % kw.pop('application_health_port'))

  if 'teardown_command' in kw:
    additional_arguments.append("--teardown_command='%s'" % kw.pop('teardown_command'))

  if 'health_check_config' in kw:
    additional_bindings['__health_check_config'] = kw.pop('health_check_config')
    additional_bindings['__health_check_ptr'] = '__health_check_config'
  else:
    # inherit from Job
    additional_bindings['__health_check_ptr'] = 'health_check_config'

  additional_arguments.extend([
      '--http_port={{thermos.ports[health]}}',
      '--timeout_secs={{{{__health_check_ptr}}.timeout_secs}}',
      '--interval_secs={{{{__health_check_ptr}}.interval_secs}}',
      '--initial_interval_secs={{{{__health_check_ptr}}.initial_interval_secs}}',
      '--max_consecutive_failures={{{{__health_check_ptr}}.max_consecutive_failures}}',
  ])

  kw['cmdline'] = dedent('''
    if [[ ! -x {{{{health_check_package}}.package}} ]]; then
      {{{{health_check_package}}.copy_command}}
      chmod +x {{{{health_check_package}}.package}}
    fi

    ./{{{{health_check_package}}.package}} %s -- %s
  ''' % (' '.join(additional_arguments), quote(kw.pop('cmdline'))))

  return Process(**kw).bind(**additional_bindings)
