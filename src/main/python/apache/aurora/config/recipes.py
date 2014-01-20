#
# Copyright 2013 Apache Software Foundation
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

import os

from .loader import AuroraConfigLoader

import pkg_resources


class Recipes(object):
  """
    Encapsulate a registry of Recipes (i.e. tasks to mutate the behavior of other tasks.)
  """
  REGISTRY = {}
  RECIPE_EXTENSION = '.aurora_recipe'

  class Error(Exception): pass
  class UnknownRecipe(Error): pass

  @classmethod
  def get(cls, name):
    if name not in cls.REGISTRY:
      raise cls.UnknownRecipe('Could not find recipe %s!' % name)
    return cls.REGISTRY[name]

  @classmethod
  def include_one(cls, filename):
    recipe_env = AuroraConfigLoader.load(filename)
    cls.REGISTRY.update(recipe_env.get('recipes', {}))

  @classmethod
  def include_module(cls, module):
    for filename in pkg_resources.resource_listdir(module, ''):
      if filename.endswith(cls.RECIPE_EXTENSION):
        cls.include_one(os.path.join(module.replace('.', os.sep), filename))

  @classmethod
  def include(cls, path):
    if os.path.isfile(path):
      cls.include_one(path)
    elif os.path.isdir(path):
      for filename in os.listdir(path):
        if filename.endswith(cls.RECIPE_EXTENSION):
          cls.include_one(os.path.join(path, filename))
    else:
      raise ValueError('Could not find %s' % path)
