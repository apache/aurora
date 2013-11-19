python_test_suite(name = 'all',
  dependencies = [
    pants(':binding_helper'),
    pants(':config'),
    pants('src/test/python/twitter/aurora/client/api:all'),
    pants('src/test/python/twitter/aurora/client/commands:all'),
    pants('src/test/python/twitter/aurora/client/hooks:all'),
  ],
)

python_tests(
  name = 'binding_helper',
  sources = ['test_binding_helper.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:pystachio'),
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/main/python/twitter/aurora/client:binding_helper'),
    pants('src/main/python/twitter/aurora/config'),
  ],
)

python_tests(name = 'config',
  sources = ['test_config.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:mox'),
    pants('src/main/python/twitter/aurora/client:config'),
  ],
)

python_library(
  name = 'fake_scheduler_proxy',
  sources = ['fake_scheduler_proxy.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/client/api:scheduler_client'),
  ]
)
