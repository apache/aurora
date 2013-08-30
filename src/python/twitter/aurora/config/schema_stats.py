from twitter.thermos.config.schema import *

def Stats(port = 'http', library = 'metrics', version = 'live'):
  config = {
    # This copies the agent distribution package from packer into the sandbox directory
    'unpack': '{{packer[{{role}}][absorber][' + version + '].copy_command}}',
    'configure': 'chmod +x absorber',
    'portname': port,
    'library': library
  }

  assert library in ["metrics", "ostrich"], (
    "The value for 'library' argument to Stats agent must be one of: 'metrics' or 'ostrich'.")

  return Process(
    name = 'stats',
    cmdline = '''if [[ ! -x absorber ]]; then
                    {{__stats_config.unpack}} &&
                    {{__stats_config.configure}}
                 fi && \
                 ./absorber -pull -role={{role}} \
                 -env={{environment}} -source={{mesos.instance}} \
                 -zone={{cluster}} -job={{super.super.name}} \
                 -pullport={{thermos.ports[{{__stats_config.portname}}]}} \
                 -pulllibrary={{__stats_config.library}}
              ''',
    daemon = True, # restart when exits
    max_failures = 10,
    ephemeral = True
  ).bind(__stats_config = config)
