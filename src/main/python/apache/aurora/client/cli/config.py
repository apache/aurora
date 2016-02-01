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

"""
An implementation of a "config" noun, for commands that
operate in on configuration files.
"""

from __future__ import print_function

from apache.aurora.client.cli import EXIT_COMMAND_FAILURE, EXIT_OK, Noun, Verb
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import BIND_OPTION, CONFIG_ARGUMENT, JSON_READ_OPTION
from apache.aurora.client.config import AuroraConfig
from apache.aurora.config.loader import AuroraConfigLoader


class ListJobsCommand(Verb):
  @property
  def name(self):
    return 'list'

  @property
  def help(self):
    return "List all of the jobs defined in a configuration file"

  def get_options(self):
    return [BIND_OPTION, CONFIG_ARGUMENT, JSON_READ_OPTION]

  def execute(self, context):
    def maybe_bind(j):
      return j.bind(*bindings) if bindings else j

    def get_jobkey(job):
      return "/".join([job.cluster().get(), job.role().get(), job.environment().get(),
          job.name().get()])

    try:
      if context.options.read_json:
        env = AuroraConfigLoader.load_json(context.options.config_file)
      else:
        env = AuroraConfigLoader.load(context.options.config_file)
    except (AuroraConfig.Error, AuroraConfigLoader.Error, ValueError) as e:
      context.print_err("Error loading configuration file: %s" % e)
      return EXIT_COMMAND_FAILURE
    bindings = context.options.bindings
    job_list = env.get("jobs", [])
    if not job_list:
      context.print_out("jobs=[]")
    else:
      bound_jobs = map(maybe_bind, job_list)
      job_names = map(get_jobkey, bound_jobs)
      context.print_out("jobs=[%s]" % (", ".join(job_names)))
    return EXIT_OK


class ConfigNoun(Noun):
  @property
  def name(self):
    return 'config'

  @property
  def help(self):
    return "Work with an aurora configuration file"

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(ConfigNoun, self).__init__()
    self.register_verb(ListJobsCommand())
