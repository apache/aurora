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

from pystachio import Ref
from pystachio.matcher import Any, Matcher
from twitter.common.contextutil import temporary_file

from apache.aurora.client import binding_helper
from apache.aurora.client.binding_helper import BindingHelper, CachingBindingHelper
from apache.aurora.config import AuroraConfig

GENERIC_CONFIG = """
main_job = Job(
  name = 'hello_world',
  environment = 'prod',
  cluster = 'derp',
  task = Task(
    name = 'main',
    processes = [
      Process(name='hello_one', cmdline='{{uncached[hello]}} {{cached[hello]}}'),
      Process(name='hello_two', cmdline='{{uncached[hello]}} {{cached[hello]}}')
    ],
    resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB),
  )
)

jobs = [
  main_job(role = 'john_doe'),
  main_job(role = 'jane_doe'),
]
"""


class UncachedHelper(BindingHelper):
  @property
  def name(self):
    return 'uncached'

  @property
  def matcher(self):
    return Matcher('uncached')[Any]

  def __init__(self):
    self.binds = 0
    super(UncachedHelper, self).__init__()

  def bind(self, config, match, env, binding_dict):
    # TODO(wickman) You should be able to take a match tuple + matcher
    # object and return the ref.
    self.binds += 1
    ref = Ref.from_address('%s[%s]' % match)
    config.bind({ref: 'U(%s)' % match[1]})


class CachedHelper(CachingBindingHelper):
  @property
  def name(self):
    return 'cached'

  @property
  def matcher(self):
    return Matcher('cached')[Any]

  def __init__(self):
    self.binds, self.uncached_binds = 0, 0
    super(CachedHelper, self).__init__()

  def bind(self, *args, **kw):
    self.binds += 1
    super(CachedHelper, self).bind(*args, **kw)

  def uncached_bind(self, config, match, env, binding_dict):
    self.uncached_binds += 1
    ref = Ref.from_address('%s[%s]' % match)
    binding = {ref: 'C(%s)' % match[1]}
    config.bind(binding)
    return binding


def write_and_load_config(role):
  with temporary_file() as fp:
    fp.write(GENERIC_CONFIG)
    fp.flush()
    return AuroraConfig.load(fp.name, name='hello_world', select_role=role)


def test_registry():
  binding_helper.unregister_all()
  assert len(binding_helper._BINDING_HELPERS) == 0

  BindingHelper.register(UncachedHelper())
  assert len(binding_helper._BINDING_HELPERS) == 1

  BindingHelper.register(CachedHelper())
  assert len(binding_helper._BINDING_HELPERS) == 2

  binding_helper.unregister_all()
  assert len(binding_helper._BINDING_HELPERS) == 0


def test_helper_types():
  binding_helper.unregister_all()
  BindingHelper.register(UncachedHelper())
  BindingHelper.register(CachedHelper())

  uncached_helper = binding_helper._BINDING_HELPERS[0]
  cached_helper = binding_helper._BINDING_HELPERS[1]

  def smoke_test():
    cfg = write_and_load_config(role='john_doe')
    binding_helper.apply_all(cfg)
    assert cfg.task(0).processes()[0].cmdline().get() == 'U(hello) C(hello)'
    assert cfg.task(0).processes()[1].cmdline().get() == 'U(hello) C(hello)'

  # initially everything uncached (also -- multiple symbols result in a single bind)
  smoke_test()
  assert uncached_helper.binds == 1
  assert cached_helper.binds == 1
  assert cached_helper.uncached_binds == 1

  # cached helper is warm, should no longer have uncached binds
  smoke_test()
  assert uncached_helper.binds == 2
  assert cached_helper.binds == 2
  assert cached_helper.uncached_binds == 1

  # flush cache and validate miss
  binding_helper.clear_binding_caches()
  smoke_test()
  assert uncached_helper.binds == 3
  assert cached_helper.binds == 3
  assert cached_helper.uncached_binds == 2
