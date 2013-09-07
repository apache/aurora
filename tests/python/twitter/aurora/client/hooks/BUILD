python_test_suite(name = 'all',
  dependencies = [
    pants(':api_hookable'),
    pants(':non_hooked_api'),
  ],
)

python_tests(name = 'api_hookable',
  sources = ['test_api_hookable.py'],
  dependencies = [
    pants('src/python/twitter/aurora/client:api'),
    pants('src/python/twitter/aurora/client/hooks:hooked_api'),
    pants('3rdparty/python:mock'),
  ]
)

python_tests(name = 'non_hooked_api',
  sources = ['test_non_hooked_api.py'],
  dependencies = [
    pants('src/python/twitter/aurora/client:config'),
    pants('src/python/twitter/aurora/client/hooks:hooked_api'),
    pants('3rdparty/python:mock'),
  ],
)
