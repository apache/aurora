from apache.aurora.client.cli import CommandOption
from apache.aurora.common.aurora_job_key import AuroraJobKey


BIND_OPTION = CommandOption('--bind', type=str, default=[], dest='bindings',
    action='append',
    help='Bind a thermos mustache variable name to a value. '
    'Multiple flags may be used to specify multiple values.')


BROWSER_OPTION = CommandOption('--open-browser', default=False, dest='open_browser',
    action='store_true',
    help='open browser to view job page after job is created')


CONFIG_ARGUMENT = CommandOption('config_file', type=str,
    help='pathname of the aurora configuration file contain the job specification')


JOBSPEC_ARGUMENT = CommandOption('jobspec', type=AuroraJobKey.from_path,
    help='Fully specified job key, in CLUSTER/ROLE/ENV/NAME format')


JSON_OPTION = CommandOption('--json', default=False, dest='json', action='store_true',
    help='Read job configuration in json format')
