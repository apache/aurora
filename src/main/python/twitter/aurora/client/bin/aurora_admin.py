from twitter.aurora.client.base import generate_terse_usage
from twitter.aurora.client.commands import admin, help
from twitter.aurora.client.options import add_verbosity_options
from twitter.common import app
from twitter.common.log.options import LogOptions


app.register_commands_from(admin, help)
add_verbosity_options()


def main():
  app.help()


LogOptions.set_stderr_log_level('INFO')
LogOptions.disable_disk_logging()
app.set_name('aurora-admin')
app.set_usage(generate_terse_usage())


def proxy_main():
  app.main()
