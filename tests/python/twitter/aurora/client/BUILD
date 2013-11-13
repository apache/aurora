python_test_suite(name = 'all',
  dependencies = [
    pants(':binding_helper'),
    pants(':config'),
    pants('tests/python/twitter/aurora/client/api:all'),
    pants('tests/python/twitter/aurora/client/api_internal:all'),
    pants('tests/python/twitter/aurora/client/binding_helpers:all'),
    pants('tests/python/twitter/aurora/client/commands:all'),
    pants('tests/python/twitter/aurora/client/commands_internal:all'),
    pants('tests/python/twitter/aurora/client/hooks:all'),
  ],
)

python_tests(
  name = 'binding_helper',
  sources = ['test_binding_helper.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/aurora/client:binding_helper'),
    pants('src/python/twitter/aurora/config'),
    pants('src/python/twitter/common/contextutil'),
  ],
)

python_tests(name = 'config',
  sources = ['test_config.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/client:config'),
  ],
)

python_library(
  name = 'fake_scheduler_proxy',
  sources = ['fake_scheduler_proxy.py'],
  dependencies = [
    pants('src/python/twitter/aurora/client/api:scheduler_client'),
  ]
)
