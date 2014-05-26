#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from twitter.common import app
from twitter.common.log.options import LogOptions

from apache.aurora.client.base import generate_terse_usage
from apache.aurora.client.commands import admin, help, maintenance
from apache.aurora.client.options import add_verbosity_options

app.register_commands_from(admin, help, maintenance)
add_verbosity_options()


def main():
  app.help()


LogOptions.set_stderr_log_level('INFO')
LogOptions.disable_disk_logging()
app.set_name('aurora-admin')
app.set_usage(generate_terse_usage())


def proxy_main():
  app.main()
