python_test_suite(name = 'all',
  dependencies = [
    pants(':config'),
    pants('tests/python/twitter/aurora/client/api:all'),
    pants('tests/python/twitter/aurora/client/api_internal:all'),
    pants('tests/python/twitter/aurora/client/commands:all'),
    pants('tests/python/twitter/aurora/client/commands_internal:all'),
    pants('tests/python/twitter/aurora/client/hooks:all'),
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
