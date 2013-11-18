python_library(
  name = 'json',
  sources = ['json.py'],
  dependencies = [
    pants('src/python/twitter/common/http'),
  ]
)

python_library(
  name = 'static_assets',
  sources = ['static_assets.py'],
  resources = rglobs('assets/*'),
  dependencies = [
    pants('src/python/twitter/thermos:bottle'),
  ]
)

python_library(
  name = 'templating',
  sources = ['templating.py'],
  resources = globs('templates/*.tpl'),
)

python_library(
  name = 'file_browser',
  sources = ['file_browser.py'],
  dependencies = [
    pants(':templating'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/python/twitter/common/http'),
    pants('src/python/twitter/thermos:bottle'),
    pants('src/python/twitter/thermos:mako'),
  ]
)

python_library(
  name = 'http_observer',
  sources = ['__init__.py', 'http_observer.py'],
  dependencies = [
    pants(':file_browser'),
    pants(':json'),
    pants(':static_assets'),
    pants(':templating'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/python/twitter/common/http'),
  ]
)
