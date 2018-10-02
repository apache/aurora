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
import hashlib
import json
import mock
import os
import tempfile
from io import BytesIO

import pytest
from twitter.common.contextutil import temporary_file, temporary_dir

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
  environment = 'devel',
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
  environment = 'devel',
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

MESOS_CONFIG_WITH_INCLUDE_TEMPLATE = JOB2 + """
include("./%s")
jobs = [HELLO_WORLD, OTHERJOB]
"""

MESOS_CONFIG_MD5 = hashlib.md5(MESOS_CONFIG).hexdigest()


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


def test_gen_content_key():
  content = "one two three"
  expected_md5 = hashlib.md5(content).hexdigest()

  assert AuroraConfigLoader.gen_content_key(1) is None, (
    "Non filetype results in None")

  with temporary_dir() as d:
    filename = os.path.join(d, 'file')
    assert AuroraConfigLoader.gen_content_key(filename) is None, (
      "non existant file results in key=None")

    with open(filename, 'w+') as fp:
      fp.write(content)
      fp.flush()
      fp.seek(0)
      for config in (fp.name, fp):
        assert expected_md5 == AuroraConfigLoader.gen_content_key(config), (
          "check hexdigest for %s" % config)


@mock.patch('apache.aurora.config.loader.AuroraConfigLoader.gen_content_key')
def test_memoized_load_json_cache_hit(mock_gen_content_key):
  expected_env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))
  expected_job_json = json.dumps(expected_env['jobs'][0].get())
  mock_gen_content_key.return_value = MESOS_CONFIG_MD5
  AuroraConfigLoader.CACHED_JSON = {MESOS_CONFIG_MD5: expected_job_json}
  loaded_job_json = AuroraConfigLoader.load_json('a/path', is_memoized=True)
  assert loaded_job_json == expected_job_json, "Test cache hit load_json"


def test_load_json_memoized():
  AuroraConfigLoader.CACHED_JSON = {}
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG_MULTI))
  jobs = env['jobs']
  content = json.dumps(jobs[0].get())
  expected_md5 = hashlib.md5(content).hexdigest()
  with temporary_dir() as d:
    filename = os.path.join(d, 'config.json')
    with open(filename, 'w+') as fp:
      fp.write(json.dumps(jobs[0].get()))
      fp.close()
      loaded_job = AuroraConfigLoader.load_json(fp.name, is_memoized=False)['jobs'][0]
      assert loaded_job == jobs[0]
      assert expected_md5 not in AuroraConfigLoader.CACHED_JSON, (
        "No key is cached when is_memoized=False")

      loaded_job = AuroraConfigLoader.load_json(fp.name, is_memoized=True)['jobs'][0]
      assert loaded_job == jobs[0]
      assert expected_md5 in AuroraConfigLoader.CACHED_JSON, (
        "Key is cached when is_memoized=True")


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


def test_load_with_includes():
  with temporary_dir() as tmp_dir:
    f1_name = 'f1.aurora'
    f2_name = 'f2.aurora'
    with open(os.path.join(tmp_dir, f1_name), 'w+') as f1:
      f1.write(MESOS_CONFIG)
      f1.flush()
      f1.seek(0)
      with open(os.path.join(tmp_dir, f2_name), 'w+') as f2:
        f2.write(MESOS_CONFIG_WITH_INCLUDE_TEMPLATE % f1_name)
        f2.flush()
        f2.seek(0)

        env = AuroraConfigLoader.load(f2.name, is_memoized=True)
        assert 'jobs' in env and len(env['jobs']) == 2
        hello_world = env['jobs'][0]
        assert hello_world.name().get() == 'hello_world'
        other_job = env['jobs'][1]
        assert other_job.name().get() == 'otherjob'


@mock.patch('apache.aurora.config.loader.AuroraConfigLoader.gen_content_key')
def test_memoized_load_cache_hit(mock_gen_content_key):
  expected_env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))
  mock_gen_content_key.return_value = MESOS_CONFIG_MD5
  AuroraConfigLoader.CACHED_ENV = {MESOS_CONFIG_MD5: expected_env}
  loaded_env = AuroraConfigLoader.load('a/path', is_memoized=True)
  assert loaded_env == expected_env, "Test cache hit"


def test_memoized_load():
  AuroraConfigLoader.CACHED_ENV = {}
  def check_env(env, config):
    assert 'jobs' in env and len(env['jobs']) == 1, (
      "Match expected jobs for config=%s" % config)
    assert env['jobs'][0].name().get() == 'hello_world'

  with temporary_dir() as d:
    with open(os.path.join(d, 'config.aurora'), 'w+') as fp:
      fp.write(MESOS_CONFIG)
      fp.flush()
      fp.seek(0)

      for config in (fp.name, fp):
        AuroraConfigLoader.CACHED_ENV = {}
        env = AuroraConfigLoader.load(config, is_memoized=False)
        check_env(env, config)
        assert MESOS_CONFIG_MD5 not in AuroraConfigLoader.CACHED_ENV.keys(), (
          "No key is cached when config=%s and is_memoized=False")

        fp.seek(0)  # previous load results in filepointer at eof
        env = AuroraConfigLoader.load(config, is_memoized=True)
        check_env(env, config)
        assert MESOS_CONFIG_MD5 in AuroraConfigLoader.CACHED_ENV.keys(), (
          "Key is cached when config=%s and is_memoized=True" % config)


def test_pick():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))

  hello_world = env['jobs'][0]
  assert AuroraConfig.pick(env, 'hello_world', None) == hello_world

  env['jobs'][0] = env['jobs'][0](name='something_{{else}}')
  assert str(AuroraConfig.pick(env, 'something_else', [{'else': 'else'}]).name()) == (
      'something_else')
