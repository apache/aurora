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
