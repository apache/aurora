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
import pkgutil

from pystachio.config import Config as PystachioConfig

from apache.aurora.config.schema import base as base_schema


class AuroraConfigLoader(PystachioConfig):
  SCHEMA_MODULES = []

  @classmethod
  def assembled_schema(cls, schema_modules):
    default_schema = [super(AuroraConfigLoader, cls).DEFAULT_SCHEMA]
    default_schema.extend('from %s import *' % module.__name__ for module in schema_modules)
    return '\n'.join(default_schema)

  @classmethod
  def register_schema(cls, schema_module):
    """Register the schema defined in schema_module, equivalent to doing

         from schema_module.__name__ import *

       before all pystachio configurations are evaluated.
    """
    cls.SCHEMA_MODULES.append(schema_module)
    cls.DEFAULT_SCHEMA = cls.assembled_schema(cls.SCHEMA_MODULES)

  @classmethod
  def register_schemas_from(cls, package):
    """Register schemas from all modules in a particular package."""
    for _, submodule, is_package in pkgutil.iter_modules(package.__path__):
      if is_package:
        continue
      cls.register_schema(
          __import__('%s.%s' % (package.__name__, submodule), fromlist=[package.__name__]))

  @classmethod
  def flush_schemas(cls):
    """Flush all schemas from AuroraConfigLoader.  Intended for test use only."""
    cls.SCHEMA_MODULES = []
    cls.register_schema(base_schema)

  @classmethod
  def load(cls, loadable):
    return cls.load_raw(loadable).environment

  @classmethod
  def load_raw(cls, loadable):
    return cls(loadable)

  @classmethod
  def load_json(cls, filename):
    with open(filename) as fp:
      return cls.loads_json(fp.read())

  @classmethod
  def loads_json(cls, string):
    parsed = json.loads(string)
    if 'jobs' not in parsed:
      # Convert the legacy single-job format
      parsed = {'jobs': [parsed]}

    parsed.update({
      'jobs': [base_schema.Job(x) for x in parsed.get('jobs')]
    })
    return parsed


AuroraConfigLoader.flush_schemas()
