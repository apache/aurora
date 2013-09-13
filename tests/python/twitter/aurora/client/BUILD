python_test_suite(name = 'all',
  dependencies = [
    pants(':config'),
    pants(':packercli'),
    pants('tests/python/twitter/aurora/client/api:all'),
    pants('tests/python/twitter/aurora/client/api_internal:all'),
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

python_tests(name = 'packercli',
  sources = [ 'test_packer_cli.py' ],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora:aurora_client_lib'),
  ],
)

python_library(
  name = 'fake_scheduler_proxy',
  sources = ['fake_scheduler_proxy.py'],
  dependencies = [
    pants('src/python/twitter/aurora/client/api:scheduler_client'),
  ]
)