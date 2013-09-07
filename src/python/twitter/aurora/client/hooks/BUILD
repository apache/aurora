python_library(name = 'hooks',
  dependencies = [
    pants(':base'),
    pants(':hooked_api'),
  ]
)

python_library(name = 'base',
  sources = ['__init__.py'],
)

python_library(name = 'hooked_api',
  sources = ['hooked_api.py'],
  dependencies = [
    pants(':base'),
    pants('src/python/twitter/aurora/client:api'),
  ]
)
