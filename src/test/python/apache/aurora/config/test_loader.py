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

import json
import tempfile
from io import BytesIO

import pytest
from twitter.common.contextutil import temporary_file

from apache.aurora.config import AuroraConfig
from apache.aurora.config.loader import AuroraConfigLoader


BAD_MESOS_CONFIG = """
3 2 1 3 2 4 2 3
"""

JOB1 = """
HELLO_WORLD = MesosJob(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = 'echo {{mesos.instance}}')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)
"""

JOB2 = """
OTHERJOB = MesosJob(
  name = 'otherjob',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'otherthing', cmdline = 'echo {{mesos.instance}}')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)
"""

MESOS_CONFIG = JOB1 + """
jobs = [HELLO_WORLD]
"""

MESOS_CONFIG_MULTI = JOB1 + JOB2 + """
jobs = [HELLO_WORLD, OTHERJOB]
"""


def test_enoent():
  nonexistent_file = tempfile.mktemp()
  with pytest.raises(AuroraConfigLoader.NotFound):
    AuroraConfigLoader.load(nonexistent_file)


def test_bad_config():
  with pytest.raises(AuroraConfigLoader.InvalidConfigError):
    AuroraConfigLoader.load(BytesIO(BAD_MESOS_CONFIG))


def test_filter_schema():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))
  job_dict = env['jobs'][0].get()
  job_dict['unknown_attribute'] = 'foo bar baz'
  job_json_string = json.dumps(job_dict)
  with pytest.raises(AttributeError):
    AuroraConfigLoader.loads_json(job_json_string)


def test_empty_config():
  AuroraConfigLoader.load(BytesIO())


def test_load_json_single():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))
  job = env['jobs'][0]
  new_job = AuroraConfigLoader.loads_json(json.dumps(job.get()))['jobs'][0]
  assert new_job == job


def test_load_json_multi():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG_MULTI))
  jobs = env['jobs']
  json_env = AuroraConfigLoader.loads_json(json.dumps({'jobs': [job.get() for job in jobs]}))
  json_jobs = json_env['jobs']
  assert jobs == json_jobs


def test_load():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()
    fp.seek(0)

    for config in (fp.name, fp):
      env = AuroraConfigLoader.load(config)
      assert 'jobs' in env and len(env['jobs']) == 1
      hello_world = env['jobs'][0]
      assert hello_world.name().get() == 'hello_world'


def test_pick():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))

  hello_world = env['jobs'][0]
  assert AuroraConfig.pick(env, 'hello_world', None) == hello_world

  env['jobs'][0] = env['jobs'][0](name='something_{{else}}')
  assert str(AuroraConfig.pick(env, 'something_else', [{'else': 'else'}]).name()) == (
      'something_else')
